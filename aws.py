import boto3
import time
import yaml
import sys


DEFAULT_REGION = 'us-east-1'
DEFAULT_TIMEOUT = 60
DEFAULT_DELAY = 5

# Amazon Linux 2
IMAGE_ID = 'ami-0603cbe34fd08cb81'

# Example configuration for Amazon Linux 2 AMI
example_conf = '''---
server:
  instance_type: t2.micro
  volumes:
  - device: /dev/xvda
    size_gb: 10
    type: ext4
    mount: /
  - device: /dev/xvdf
    size_gb: 100
    type: xfs
    mount: /data
  users:
  - login: user1
    ssh_key: ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC66s5Gc+C1epQp6FwB7HQNcSnbgZbmTeWte2i7YXaXTfWGABebBFY+4K6qsMk0CRA7wjeo6R673TtMG1e8yIbXqw+gIGg0DTeKwwDPOIbOng2rUuSjSMjjVtFnjxI5dy6kzuZljLpWwwfjIc8Iie/um552ObQsQz04LIDHGb6l9LVmYsMDF0rLyU+rsd6PEsaX/nlsyGb7zf6bhWVTeVle3HDO757QbMVuN9pZyNxElnHaVm9xkz2X5VdCChkCmw6Q04EGJUQT6lc70o7QXJgip7vCAJ91KQklQf/KcXk7zlkoYdenYgo+Rwo4c1P7FlPJnudWRJbhXVveFNiyOiJH user1@ip-172-31-0-65.us-east-2.compute.internal'''


class MyTerra:
    def __init__(self, region):
        if len(region) != 0:
            self.region = region
        else:
            self.region = DEFAULT_REGION
        self.session = boto3.Session(region_name=self.region)
        self.ec2 = self.session.resource('ec2')
        self.client = self.session.client('ec2')

    def create_instances(self, params):
        volumes = []
        for vol in params['volumes']:
            volumes.append(
                {
                    'DeviceName': vol['device'],
                    'Ebs': {
                        'VolumeSize': vol['size_gb'],
                    },
                }
            )

        response = self.client.run_instances(
            ImageId=params['image_id'],
            InstanceType=params['instance_type'],
            MinCount=params['min_count'],
            MaxCount=params['max_count'],
            UserData=params['user_data'],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'foo',
                            'Value': 'bar'
                        },
                    ]
                },
            ],
            BlockDeviceMappings=volumes,
        )
        for instance in response['Instances']:
            print('Created instance: {}'.format(instance['InstanceId']))

    def terminate_instances(self, term_list):
        if len(term_list) == 0:
            print('Empty list of instances IDs for termination')
            return
        count = 0
        while count <= DEFAULT_TIMEOUT:
            response = self.client.terminate_instances(
                InstanceIds=term_list,
            )
            pending_instances = []
            for inst in response['TerminatingInstances']:
                if inst['CurrentState']['Name'] != 'terminated':
                    print('Waiting for instance {} termination'.format(inst['InstanceId']))
                    pending_instances.append(inst['InstanceId'])
            if len(pending_instances) != 0:
                time.sleep(DEFAULT_DELAY)
            else:
                break
            count += 1
            if count == DEFAULT_TIMEOUT and len(pending_instances) != 0:
                raise Exception('Failed to terminate instances:', pending_instances)


if __name__ == '__main__':
    # initialize config file
    if len(sys.argv) != 2:
        params_file = sys.argv[0].split('.')[0] + '.yaml'
        with open(params_file, 'w') as file:
            file.write(example_conf)
    else:
        params_file = sys.argv[1]

    # load config file
    with open(params_file, 'r') as file:
        instance_params = yaml.load(file, Loader=yaml.FullLoader)

    # main part
    my_site = MyTerra("us-east-2")

    # sanity check for root device
    response = my_site.client.describe_images(ImageIds=[IMAGE_ID])
    for vol in instance_params['server']['volumes']:
        if vol['mount'] == '/':
            break
    if response['Images'][0]['RootDeviceName'] != vol['device']:
        raise Exception('Wrong configuration: check root device in image and config')

    # clean from previous session
    instance_ids = []
    for instance in my_site.ec2.instances.all():
        if not instance:
            continue
        if not instance.tags:
            continue
        for tag in instance.tags:
            if not tag:
                continue
            if tag['Key'] == 'foo' and tag['Value'] == 'bar':
                instance_ids.append(instance.id)

    my_site.terminate_instances(instance_ids)

    # create user data
    user_data = '#cloud-config\n'
    user_data += 'users:\n'
    for user_entry in instance_params['server']['users']:
        user = user_entry['login']
        user_data += '  - name: {}\n'.format(user)
        user_data += '    gecos: ""\n'
        user_data += '    sudo: ALL=(ALL) NOPASSWD:ALL\n'
        user_data += '    ssh_authorized_keys:\n'
        user_data += '      - {}\n'.format(user_entry['ssh_key'])
    user_data += 'runcmd:\n'
    user_data += '  - echo "Creating FS and mount additional storage"\n'
    for vol in instance_params['server']['volumes']:
        if vol['mount'] != '/':
            user_data += '  - mkfs -t {fs} {dev}\n'.format(fs=vol['type'], dev=vol['device'])
            user_data += '  - mkdir -p {mnt}\n'.format(mnt=vol['mount'])
            user_data += '  - mount {dev} {mnt}\n'.format(dev=vol['device'], mnt=vol['mount'])
            user_data += '  - echo "{dev} {mnt} {fs} defaults,noatime 1 1" >> /etc/fstab\n'.format(
                dev=vol['device'],
                mnt=vol['mount'],
                fs=vol['type']
            )
    user_data += '  - echo "Finished"\n'

    # create flavor
    # NOTE: do not support change FS type for root mount
    free_ubuntu_flavor = {
        'image_id': IMAGE_ID,
        'instance_type': 't2.micro',
        'min_count': 1,
        'max_count': 1,
        'user_data': user_data,
        'volumes': instance_params['server']['volumes'],
    }

    # create new instance
    my_site.create_instances(free_ubuntu_flavor)

