#!/usr/bin/python
# This file is part of Ansible
# Wait for this pull request https://github.com/ansible/ansible-modules-core/pull/5412
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
DOCUMENTATION = """
---
module: ec2_asg
short_description: Create or delete AWS Autoscaling Groups
description:
  - Can create or delete AWS Autoscaling Groups
  - Works with the ec2_lc module to manage Launch Configurations
version_added: "1.6"
author: "Gareth Rushgrove (@garethr)"
options:
  state:
    description:
      - register or deregister the instance
    required: false
    choices: ['present', 'absent']
    default: present
  name:
    description:
      - Unique name for group to be created or deleted
    required: true
  load_balancers:
    description:
      - List of ELB names to use for the auto scaling group
    required: false
  target_groups:
    description:
      - List of Target Group arns to use for the auto scaling group
    required: false
  availability_zones:
    description:
      - List of availability zone names in which to create the group.  Defaults to all the availability zones in the region if vpc_zone_identifier is not set.
    required: false
  launch_config_name:
    description:
      - Name of the Launch configuration to use for the group. See the ec2_lc module for managing these.
    required: true
  min_size:
    description:
      - Minimum number of instances in group, if unspecified then the current group value will be used.
    required: false
  max_size:
    description:
      - Maximum number of instances in group, if unspecified then the current group value will be used.
    required: false
  desired_capacity:
    description:
      - Desired number of instances in group, if unspecified then the current group value will be used.
    required: false
  replace_all_instances:
    description:
      - In a rolling fashion, replace all instances with an old launch configuration with one from the current launch configuration.
    required: false
    version_added: "1.8"
    default: False
  replace_batch_size:
    description:
      - Number of instances you'd like to replace at a time.  Used with replace_all_instances.
    required: false
    version_added: "1.8"
    default: 1
  replace_instances:
    description:
      - List of instance_ids belonging to the named ASG that you would like to terminate and be replaced with instances matching the current launch configuration.
    required: false
    version_added: "1.8"
    default: None
  lc_check:
    description:
      - Check to make sure instances that are being replaced with replace_instances do not already have the current launch_config.
    required: false
    version_added: "1.8"
    default: True
  vpc_zone_identifier:
    description:
      - List of VPC subnets to use
    required: false
    default: None
  tags:
    description:
      - A list of tags to add to the Auto Scale Group. Optional key is 'propagate_at_launch', which defaults to true.
    required: false
    default: None
    version_added: "1.7"
  health_check_period:
    description:
      - Length of time in seconds after a new EC2 instance comes into service that Auto Scaling starts checking its health.
    required: false
    default: 500 seconds
    version_added: "1.7"
  health_check_type:
    description:
      - The service you want the health status from, Amazon EC2 or Elastic Load Balancer.
    required: false
    default: EC2
    version_added: "1.7"
    choices: ['EC2', 'ELB']
  default_cooldown:
    description:
      - The number of seconds after a scaling activity completes before another can begin.
    required: false
    default: 300 seconds
    version_added: "2.0"
  wait_timeout:
    description:
      - how long before wait instances to become viable when replaced.  Used in conjunction with instance_ids option.
    default: 300
    version_added: "1.8"
  wait_for_instances:
    description:
      - Wait for the ASG instances to be in a ready state before exiting.  If instances are behind an ELB, it will wait until the ELB determines all instances have a lifecycle_state of  "InService" and  a health_status of "Healthy".
    version_added: "1.9"
    default: yes
    required: False
  termination_policies:
    description:
        - An ordered list of criteria used for selecting instances to be removed from the Auto Scaling group when reducing capacity.
        - For 'Default', when used to create a new autoscaling group, the "Default"i value is used. When used to change an existent autoscaling group, the current termination policies are maintained.
    required: false
    default: Default
    choices: ['OldestInstance', 'NewestInstance', 'OldestLaunchConfiguration', 'ClosestToNextInstanceHour', 'Default']
    version_added: "2.0"
  notification_topic:
    description:
      - A SNS topic ARN to send auto scaling notifications to.
    default: None
    required: false
    version_added: "2.2"
  notification_types:
    description:
      - A list of auto scaling events to trigger notifications on.
    default: ['autoscaling:EC2_INSTANCE_LAUNCH', 'autoscaling:EC2_INSTANCE_LAUNCH_ERROR', 'autoscaling:EC2_INSTANCE_TERMINATE', 'autoscaling:EC2_INSTANCE_TERMINATE_ERROR']
    required: false
    version_added: "2.2"
extends_documentation_fragment:
    - aws
    - ec2
"""

EXAMPLES = '''
# Basic configuration

- ec2_asg:
    name: special
    load_balancers: [ 'lb1', 'lb2' ]
    availability_zones: [ 'eu-west-1a', 'eu-west-1b' ]
    launch_config_name: 'lc-1'
    min_size: 1
    max_size: 10
    desired_capacity: 5
    vpc_zone_identifier: [ 'subnet-abcd1234', 'subnet-1a2b3c4d' ]
    tags:
      - environment: production
        propagate_at_launch: no

# Rolling ASG Updates

Below is an example of how to assign a new launch config to an ASG and terminate old instances.

All instances in "myasg" that do not have the launch configuration named "my_new_lc" will be terminated in
a rolling fashion with instances using the current launch configuration, "my_new_lc".

This could also be considered a rolling deploy of a pre-baked AMI.

If this is a newly created group, the instances will not be replaced since all instances
will have the current launch configuration.

- name: create launch config
  ec2_lc:
    name: my_new_lc
    image_id: ami-lkajsf
    key_name: mykey
    region: us-east-1
    security_groups: sg-23423
    instance_type: m1.small
    assign_public_ip: yes

- ec2_asg:
    name: myasg
    launch_config_name: my_new_lc
    health_check_period: 60
    health_check_type: ELB
    replace_all_instances: yes
    min_size: 5
    max_size: 5
    desired_capacity: 5
    region: us-east-1

To only replace a couple of instances instead of all of them, supply a list
to "replace_instances":

- ec2_asg:
    name: myasg
    launch_config_name: my_new_lc
    health_check_period: 60
    health_check_type: ELB
    replace_instances:
    - i-b345231
    - i-24c2931
    min_size: 5
    max_size: 5
    desired_capacity: 5
    region: us-east-1
'''
import time
import logging as log
import traceback

from ansible.module_utils.basic import *
from ansible.module_utils.ec2 import *

log.getLogger('boto3').setLevel(log.CRITICAL)
# log.basicConfig(filename='/tmp/ansible_ec2_asg.log',level=log.DEBUG, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


try:
    import boto3

    HAS_BOTO = True
except ImportError:
    HAS_BOTO = False

ASG_ATTRIBUTES_MAP = {
    'availability_zones': 'AvailabilityZones',
    'default_cooldown': 'DefaultCooldown',
    'desired_capacity': 'DesiredCapacity',
    'health_check_period': 'HealthCheckGracePeriod',
    'health_check_type': 'HealthCheckType',
    'launch_config_name': 'LaunchConfigurationName',
    'load_balancers': 'LoadBalancerNames',
    'max_size': 'MaxSize',
    'min_size': 'MinSize',
    'name': 'AutoScalingGroupName',
    'placement_group': 'PlacementGroup',
    'target_groups': 'TargetGroupARNs',
    'termination_policies': 'TerminationPolicies',
    'vpc_zone_identifier': 'VPCZoneIdentifier'
}

ANSIBLE_ASG_UPDATABLE_ATTRIBUTES = [
    'name',
    'launch_config_name',
    'min_size',
    'max_size',
    'desired_capacity',
    'default_cooldown',
    'availability_zones',
    'health_check_type',
    'health_check_period',
    'placement_group',
    'vpc_zone_identifier'
    'termination_policies',
]

ASG_UPDATABLE_ATTRIBUTES = [
    'AutoScalingGroupName',
    'LaunchConfigurationName',
    'MinSize',
    'MaxSize',
    'DesiredCapacity',
    'DefaultCooldown',
    'AvailabilityZones',
    'HealthCheckType',
    'HealthCheckGracePeriod',
    'PlacementGroup',
    'VPCZoneIdentifier',
    'TerminationPolicies',
    'NewInstancesProtectedFromScaleIn'
]

INSTANCE_ATTRIBUTES = ('instance_id', 'health_status', 'lifecycle_state', 'launch_config_name')


def enforce_required_arguments(module):
    ''' As many arguments are not required for autoscale group deletion
        they cannot be mandatory arguments for the module, so we enforce
        them here '''
    missing_args = []
    for arg in ('min_size', 'max_size', 'launch_config_name'):
        if (module.params[arg] is None):
            missing_args.append(arg)
    if (missing_args):
        module.fail_json(
            msg="Missing required arguments for autoscaling group create/update: %s" % ",".join(missing_args))


def get_asg_by_name(asg_connection, group_name):
    asg_list = asg_connection.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name], MaxRecords=1)[
        'AutoScalingGroups']
    if (len(asg_list) > 0):
        return asg_list[0]
    return None


def elb_dreg(asg_connection, elb_connection, elb2_connection, module, group_name, instance_id):
    asg = get_asg_by_name(asg_connection, group_name)
    wait_timeout = module.params.get('wait_timeout')
    count = 1

    if (not ((asg['LoadBalancerNames'] or asg['TargetGroupARNs']) and asg['HealthCheckType'] == 'ELB')):
        return

    load_balancer_names = asg['LoadBalancerNames']
    if (load_balancer_names):
        load_balancer_descriptions = elb_connection.describe_load_balancers(LoadBalancerNames=load_balancer_names)[
            'LoadBalancerDescriptions']
        for load_balancer_description in load_balancer_descriptions:
            load_balancer_name = load_balancer_description['LoadBalancerName']
            for instance in load_balancer_description['Instances']:
                if instance['InstanceId'] == instance_id:
                    elb_connection.deregister_instances_from_load_balancer(LoadBalancerName=load_balancer_name,
                                                                           Instances=[instance])
                    log.debug("De-registering {0} from ELB {1}".format(instance_id, load_balancer_name))
                    break

    target_groups = asg['TargetGroupARNs']
    if (target_groups):
        target_groups = elb2_connection.describe_target_groups(TargetGroupArns=asg['TargetGroupARNs'])['TargetGroups']
        for target_group in target_groups:
            elb2_connection.deregister_targets(TargetGroupArn=target_group['TargetGroupArn'],
                                               Targets=[{'Id': instance_id}])

    wait_timeout = time.time() + wait_timeout
    while (wait_timeout > time.time() and count > 0):
        count = 0
        for load_balancer_name in load_balancer_names:
            lb_instances = elb_connection.describe_instance_health(LoadBalancerName=load_balancer_name)['InstanceStates']
            for i in lb_instances:
                if (i['InstanceId'] == instance_id and i['State'] == "InService"):
                    count += 1
                    log.debug("{0}: {1}".format(i['InstanceId'], i['State']))
        for target_group in target_groups:
            target_health_descriptions = \
            elb2_connection.describe_target_health(TargetGroupArn=target_group['TargetGroupArn'],
                                                   Targets=[{'Id': instance_id}])['TargetHealthDescriptions']
            for target_health_description in target_health_descriptions:
                if (target_health_description['Target']['Id'] == instance_id and
                            target_health_description['TargetHealth']['State'] == "healthy"):
                    count += 1
                    log.debug("{0}: {1}".format(target_health_description['Target']['Id'],
                                                target_health_description['TargetHealth']['State']))
        time.sleep(10)

    if (wait_timeout <= time.time()):
        # waiting took too long
        module.fail_json(msg="Waited too long for instance to deregister. {0}".format(time.asctime()))


def elb_healthy(asg_connection, elb_connection, elb2_connection, module, group_name, launch_config_name):
    healthy_instances = None
    asg = get_asg_by_name(asg_connection, group_name)
    # get healthy, inservice instances from ASG
    instances = []
    for instance in asg['Instances']:
        if (instance['LifecycleState'] == 'InService' and instance['HealthStatus'] == 'Healthy'):
            if (launch_config_name):
                if (instance['LaunchConfigurationName'] == launch_config_name):
                    instances.append({
                        'InstanceId': instance['InstanceId']
                    })
            else:
                instances.append({
                    'InstanceId': instance['InstanceId']
                })

    log.debug("ASG considers the following instances InService and Healthy: {0}".format(instances))
    log.debug("ELB instance status:")
    if (instances):
        for load_balancer_name in asg['LoadBalancerNames']:
            # we catch a race condition that sometimes happens if the instance exists in the ASG
            # but has not yet show up in the ELB
            try:
                lb_instances = \
                elb_connection.describe_instance_health(LoadBalancerName=load_balancer_name, Instances=instances)[
                    'InstanceStates']
            except botocore.exceptions.ClientError as e:
                if (e['Error']['Code'] == 'InvalidInstance'):
                    return None

                module.fail_json(msg=str(e))

            new_healthy_instances = set()
            for i in lb_instances:
                if (i['State'] == "InService"):
                    new_healthy_instances.add(i['InstanceId'])
                log.debug("{0}: {1}".format(i['InstanceId'], i['State']))
            if (healthy_instances):
                healthy_instances = healthy_instances.intersection(new_healthy_instances)
            else:
                healthy_instances = new_healthy_instances

        if (asg['TargetGroupARNs']):
            target_groups = elb2_connection.describe_target_groups(TargetGroupArns=asg['TargetGroupARNs'])['TargetGroups']
            for target_group in target_groups:
                new_healthy_instances = set()
                targets = []
                for instance in instances:
                    targets.append({
                        'Id': instance['InstanceId']
                    })
                target_health_descriptions = \
                elb2_connection.describe_target_health(TargetGroupArn=target_group['TargetGroupArn'], Targets=targets)[
                    'TargetHealthDescriptions']
                for target_health_description in target_health_descriptions:
                    if (target_health_description['TargetHealth']['State'] == "healthy"):
                        new_healthy_instances.add(target_health_description['Target']['Id'])
                    log.debug("{0}: {1}".format(target_health_description['Target']['Id'],
                                                target_health_description['TargetHealth']['State']))
                if (healthy_instances):
                    healthy_instances = healthy_instances.intersection(new_healthy_instances)
                else:
                    healthy_instances = new_healthy_instances

    if (healthy_instances):
        return len(healthy_instances)
    else:
        return 0


def wait_for_elb(asg_connection, elb_connection, elb2_connection, module, group_name, launch_config_name = None,
                 min_size = 0):
    wait_timeout = module.params.get('wait_timeout')

    # if the health_check_type is ELB, we want to query the ELBs directly for instance
    # status as to avoid health_check_grace period that is awarded to ASG instances
    asg = get_asg_by_name(asg_connection, group_name)

    if ((asg['TargetGroupARNs'] or asg['LoadBalancerNames']) and asg['HealthCheckType'] == 'ELB'):
        log.debug("Waiting for ELB to consider instances healthy.")

        wait_timeout = time.time() + wait_timeout
        healthy_instances = elb_healthy(asg_connection, elb_connection, elb2_connection, module, group_name,
                                        launch_config_name)

        if (launch_config_name == None):
            min_size = asg['MinSize']
        while (healthy_instances < min_size and wait_timeout > time.time()):
            healthy_instances = elb_healthy(asg_connection, elb_connection, elb2_connection, module, group_name,
                                            launch_config_name)
            log.debug("ELB thinks {0} instances are healthy.".format(healthy_instances))
            time.sleep(10)
        if (wait_timeout <= time.time()):
            # waiting took too long
            module.fail_json(msg="Waited too long for ELB instances to be healthy. %s" % time.asctime())
        log.debug("Waiting complete.  ELB thinks {0} instances are healthy.".format(healthy_instances))


def create_autoscaling_group(asg_connection, ec2_connection, elb_connection, elb2_connection, module):
    group_name = module.params.get('name')
    load_balancers = module.params['load_balancers']
    target_groups = module.params['target_groups']
    availability_zones = module.params['availability_zones']
    launch_config_name = module.params.get('launch_config_name')
    min_size = module.params['min_size']
    max_size = module.params['max_size']
    desired_capacity = module.params.get('desired_capacity')
    vpc_zone_identifier = module.params.get('vpc_zone_identifier')
    tags = module.params.get('tags')
    health_check_period = module.params.get('health_check_period')
    health_check_type = module.params.get('health_check_type')
    default_cooldown = module.params.get('default_cooldown')
    wait_for_instances = module.params.get('wait_for_instances')
    wait_timeout = module.params.get('wait_timeout')
    termination_policies = module.params.get('termination_policies')
    notification_topic = module.params.get('notification_topic')
    notification_types = module.params.get('notification_types')

    asg = get_asg_by_name(asg_connection, group_name)

    if (not vpc_zone_identifier and not availability_zones):
        region, ec2_url, aws_connect_params = get_aws_connection_info(module)
    elif (vpc_zone_identifier):
        vpc_zone_identifier = ','.join(vpc_zone_identifier)

    asg_tags = []
    if (tags != None):
        for tag in tags:
            for k, v in tag.items():
                if (k != 'propagate_at_launch'):
                    asg_tags.append({
                        'Key': k,
                        'Value': v,
                        'ResourceType': 'auto-scaling-group',
                        'ResourceId': group_name,
                        'PropagateAtLaunch': bool(tag.get('propagate_at_launch', True))
                    })

    if (not asg):
        if (not vpc_zone_identifier and not availability_zones):
            availability_zones = [zone['ZoneName'] for zone in
                                  ec2_connection.describe_availability_zones()['AvailabilityZones']]
            module.params['availability_zones'] = availability_zones
        enforce_required_arguments(module)
        launch_config = asg_connection.describe_launch_configurations(LaunchConfigurationNames=[launch_config_name])[
            'LaunchConfigurations'][0]['LaunchConfigurationName']
        new_asg = {
            'AutoScalingGroupName': group_name,
            'LaunchConfigurationName': launch_config,
            'MinSize': min_size,
            'MaxSize': max_size,
            'Tags': asg_tags,
            'HealthCheckGracePeriod': health_check_period,
            'HealthCheckType': health_check_type,
            'DefaultCooldown': default_cooldown,
            'TerminationPolicies': termination_policies
        }
        if (load_balancers):
            new_asg['LoadBalancerNames'] = load_balancers
        if (target_groups):
            new_asg['TargetGroupARNs'] = target_groups
        if (desired_capacity):
            new_asg['DesiredCapacity'] = desired_capacity
        if (vpc_zone_identifier):
            new_asg['VPCZoneIdentifier'] = vpc_zone_identifier
        else:
            new_asg['AvailabilityZones'] = availability_zones

        try:
            asg_connection.create_auto_scaling_group(**new_asg)
            if wait_for_instances:
                wait_for_new_inst(module, asg_connection, group_name, wait_timeout, desired_capacity)
                wait_for_elb(asg_connection, elb_connection, elb2_connection, module, group_name, launch_config_name,
                             min_size)

            if notification_topic:
                asg_connection.put_notification_configuration(AutoScalingGroupName=group_name,
                                                              TopicARN=notification_topic,
                                                              NotificationTypes=notification_types)

            asg = get_asg_by_name(asg_connection, group_name)
            changed = True
            return (changed, asg)
        except botocore.exceptions.ClientError as e:
            module.fail_json(msg="Failed to create Autoscaling Group: %s" % str(e), exception=traceback.format_exc(e))
    else:
        changed = False
        for attr in ANSIBLE_ASG_UPDATABLE_ATTRIBUTES:
            if (module.params.get(attr, None) is not None):
                module_attr = module.params.get(attr)
                if (attr == 'vpc_zone_identifier'):
                    module_attr = ','.join(module_attr)
                group_attr = asg[ASG_ATTRIBUTES_MAP[attr]]
                # we do this because AWS and the module may return the same list
                # sorted differently
                if (attr != 'termination_policies'):
                    try:
                        module_attr.sort()
                    except:
                        pass
                    try:
                        group_attr.sort()
                    except:
                        pass
                if group_attr != module_attr:
                    changed = True
                    asg[ASG_ATTRIBUTES_MAP[attr]] = module_attr

        if (tags != None):
            existing_tags = asg['Tags']
            want_tags = asg_tags
            to_be_deleted_tags = []
            for existing_tag in existing_tags:
                check = True
                for want_tag in want_tags:
                    if (want_tag['Key'] == existing_tag['Key']):
                        check = False
                        break
                if (check):
                    to_be_deleted_tags.append(existing_tag)

            if (to_be_deleted_tags):
                args = {
                    'Tags': to_be_deleted_tags
                }
                asg_connection.delete_tags(**args)

            args = {
                'Tags': asg_tags
            }
            asg_connection.create_or_update_tags(**args)

        if (load_balancers != None):
            existing_load_balancers = set(asg['LoadBalancerNames'])
            want_load_balancers = set(load_balancers)
            to_be_detached_load_balancers = existing_load_balancers.difference(want_load_balancers)
            to_be_attached_load_balancers = want_load_balancers.difference(existing_load_balancers)
            if (to_be_detached_load_balancers):
                args = {
                    'AutoScalingGroupName': group_name,
                    'LoadBalancerNames': list(to_be_detached_load_balancers)
                }
                asg_connection.detach_load_balancers(**args)
            if (to_be_attached_load_balancers):
                args = {
                    'AutoScalingGroupName': group_name,
                    'LoadBalancerNames': list(to_be_attached_load_balancers)
                }
                asg_connection.attach_load_balancers(**args)

        if (target_groups != None):
            existing_target_groups = set(asg['TargetGroupARNs'])
            want_target_groups = set(target_groups)
            to_be_detached_target_groups = existing_target_groups.difference(want_target_groups)
            to_be_attached_target_groups = want_target_groups.difference(existing_target_groups)
            args = {
                'AutoScalingGroupName': group_name,
                'TargetGroupARNs': list(to_be_detached_target_groups)
            }
            asg_connection.detach_load_balancer_target_groups(**args)
            args = {
                'AutoScalingGroupName': group_name,
                'TargetGroupARNs': list(to_be_attached_target_groups)
            }
            asg_connection.attach_load_balancer_target_groups(**args)

        if (changed):
            try:
                updatable_asg = {}
                for key in asg:
                    if (key in ASG_UPDATABLE_ATTRIBUTES):
                        updatable_asg[key] = asg[key]
                if (updatable_asg['DesiredCapacity'] < updatable_asg['MinSize']):
                    updatable_asg['DesiredCapacity'] = updatable_asg['MinSize']
                if (updatable_asg['DesiredCapacity'] > updatable_asg['MaxSize']):
                    updatable_asg['DesiredCapacity'] = updatable_asg['MaxSize']
                asg_connection.update_auto_scaling_group(**updatable_asg)
            except botocore.exceptions.ClientError as e:
                module.fail_json(msg="Failed to update Autoscaling Group: %s" % str(e),
                                 exception=traceback.format_exc(e))

        if (notification_topic):
            try:
                asg_connection.put_notification_configuration(AutoScalingGroupName=group_name,
                                                              TopicARN=notification_topic,
                                                              NotificationTypes=notification_types)
            except botocore.exceptions.ClientError as e:
                module.fail_json(msg="Failed to update Autoscaling Group notifications: %s" % str(e),
                                 exception=traceback.format_exc(e))

        if (wait_for_instances):
            wait_for_new_inst(module, asg_connection, group_name, wait_timeout, desired_capacity)
            wait_for_elb(asg_connection, elb_connection, elb2_connection, module, group_name)
        try:
            asg = get_asg_by_name(asg_connection, group_name)
        except botocore.exceptions.ClientError as e:
            module.fail_json(msg="Failed to read existing Autoscaling Groups: %s" % str(e),
                             exception=traceback.format_exc(e))
        return (changed, asg)


def delete_autoscaling_group(asg_connection, module):
    group_name = module.params.get('name')
    notification_topic = module.params.get('notification_topic')

    if (notification_topic):
        asg_connection.delete_notification_configuration(AutoScalingGroupName=group_name, TopicARN=notification_topic)

    asg = get_asg_by_name(asg_connection, group_name)
    if (asg):
        update_size(asg_connection, asg, 0, 0, 0)
        instances = True
        while (instances):
            tmp_group = get_asg_by_name(asg_connection, group_name)
            if (tmp_group):
                if (not tmp_group['Instances']):
                    instances = False
            time.sleep(10)

        asg_connection.delete_auto_scaling_group(AutoScalingGroupName=group_name)
        while (get_asg_by_name(asg_connection, group_name)):
            time.sleep(5)
        changed = True
        return changed
    else:
        changed = False
        return changed


def get_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def update_size(asg_connection, asg, max_size, min_size, dc):
    log.debug("setting ASG sizes")
    log.debug("minimum size: {0}, desired_capacity: {1}, max size: {2}".format(min_size, dc, max_size))
    asg['MaxSize'] = max_size
    asg['MinSize'] = min_size
    asg['DesiredCapacity'] = dc
    updatable_asg = {}
    for key in asg:
        if (key in ASG_UPDATABLE_ATTRIBUTES):
            updatable_asg[key] = asg[key]
    asg_connection.update_auto_scaling_group(**updatable_asg)


def replace(asg_connection, elb_connection, elb2_connection, module):
    batch_size = module.params.get('replace_batch_size')
    wait_timeout = module.params.get('wait_timeout')
    group_name = module.params.get('name')
    launch_config_name = module.params.get('launch_config_name')
    lc_check = module.params.get('lc_check')
    replace_instances = module.params.get('replace_instances')

    asg = get_asg_by_name(asg_connection, group_name)
    wait_for_new_inst(module, asg_connection, group_name, wait_timeout, asg['MinSize'])
    instances = asg['Instances']
    if (replace_instances):
        instances = []
        for replace_instance in replace_instances:
            instances.append({'InstanceId': replace_instance})
    # check to see if instances are replaceable if checking launch configs

    # check if min_size/max_size/desired capacity have been specified and if not use ASG values
    min_size = asg['MinSize']
    max_size = asg['MaxSize']
    desired_capacity = asg['DesiredCapacity']

    new_instances, old_instances = get_instances_by_lc(asg, lc_check, instances)
    num_new_inst_needed = desired_capacity - len(new_instances)

    if (lc_check):
        if (num_new_inst_needed == 0 and old_instances):
            log.debug("No new instances needed, but old instances are present. Removing old instances")
            terminate_batch(asg_connection, elb_connection, elb2_connection, module, min_size, desired_capacity,
                            old_instances, instances, True)
            asg = get_asg_by_name(asg_connection, group_name)
            changed = True
            return (changed, asg)

        # we don't want to spin up extra instances if not necessary
        if (num_new_inst_needed < batch_size):
            log.debug("Overriding batch size to {0}".format(num_new_inst_needed))
            batch_size = num_new_inst_needed

    if (not old_instances):
        changed = False
        return (changed, asg)

    # set temporary settings and wait for them to be reached
    # This should get overwritten if the number of instances left is less than the batch size.

    minimal_instance = len(new_instances) + batch_size
    asg = get_asg_by_name(asg_connection, group_name)
    update_size(asg_connection, asg, max_size + batch_size, min_size + batch_size, desired_capacity + batch_size)
    wait_for_new_inst(module, asg_connection, group_name, wait_timeout, asg['MinSize'])
    wait_for_elb(asg_connection, elb_connection, elb2_connection, module, group_name, launch_config_name, minimal_instance)

    asg = get_asg_by_name(asg_connection, group_name)
    instances = asg['Instances']
    if (replace_instances):
        instances = []
        for replace_instance in replace_instances:
            instances.append({'InstanceId': replace_instance})

    log.debug("beginning main loop")

    for i in get_chunks(instances, batch_size):
        # break out of this loop if we have enough new instances
        break_early, desired_size, term_instances = terminate_batch(asg_connection, elb_connection, elb2_connection,
                                                                    module, min_size, desired_capacity, i, instances,
                                                                    False)
        if (not break_early):
            minimal_instance = minimal_instance + len(i)
        wait_for_term_inst(asg_connection, module, term_instances)
        wait_for_new_inst(module, asg_connection, group_name, wait_timeout, desired_size)
        wait_for_elb(asg_connection, elb_connection, elb2_connection, module, group_name, launch_config_name, minimal_instance)
        if (break_early):
            log.debug("breaking loop")
            break
    asg = get_asg_by_name(asg_connection, group_name)
    update_size(asg_connection, asg, max_size, min_size, desired_capacity)
    asg = get_asg_by_name(asg_connection, group_name)
    log.debug("Rolling update complete.")
    changed = True
    return (changed, asg)


def get_instances_by_lc(asg, lc_check, initial_instances):
    new_instances = []
    old_instances = []
    # old instances are those that have the old launch config
    if (lc_check):
        for i in asg['Instances']:
            if (i['LaunchConfigurationName'] == asg['LaunchConfigurationName']):
                new_instances.append(i)
            else:
                old_instances.append(i)

    else:
        log.debug("Comparing initial instances with current: {0}".format(initial_instances))
        for i in asg['Instances']:
            if (i not in initial_instances):
                new_instances.append(i)
            else:
                old_instances.append(i)
    log.debug("New instances: {0}, {1}".format(len(new_instances), new_instances))
    log.debug("Old instances: {0}, {1}".format(len(old_instances), old_instances))

    return new_instances, old_instances


def list_purgeable_instances(asg, lc_check, replace_instances, initial_instances):
    instances_to_terminate = []
    instances = []
    for instance in replace_instances:
        for asg_instance in asg['Instances']:
            if (instance['InstanceId'] == asg_instance['InstanceId']):
                instances.append(instance)
                break

    # check to make sure instances given are actually in the given ASG
    # and they have a non-current launch config
    if (lc_check):
        for i in instances:
            if (i['LaunchConfigurationName'] != asg['LaunchConfigurationName']):
                instances_to_terminate.append(i)
    else:
        for i in instances:
            if (i in initial_instances):
                instances_to_terminate.append(i)
    return instances_to_terminate


def terminate_batch(asg_connection, elb_connection, elb2_connection, module, min_size, desired_capacity,
                    replace_instances, initial_instances, leftovers=False):
    batch_size = module.params.get('replace_batch_size')
    group_name = module.params.get('name')
    lc_check = module.params.get('lc_check')
    decrement_capacity = False
    break_loop = False

    asg = get_asg_by_name(asg_connection, group_name)
    desired_size = asg['MinSize']

    new_instances, old_instances = get_instances_by_lc(asg, lc_check, initial_instances)
    num_new_inst_needed = desired_capacity - len(new_instances)

    # check to make sure instances given are actually in the given ASG
    # and they have a non-current launch config
    instances_to_terminate = list_purgeable_instances(asg, lc_check, replace_instances, initial_instances)

    if (num_new_inst_needed == 0):
        decrement_capacity = True
        if (asg['MinSize'] != min_size):
            asg['MinSize'] = min_size
            updatable_asg = {}
            for key in asg:
                if key in ASG_UPDATABLE_ATTRIBUTES:
                    updatable_asg[key] = asg[key]
            asg_connection.update_auto_scaling_group(**updatable_asg)
            log.debug("Updating minimum size back to original of {0}".format(min_size))
        # if are some leftover old instances, but we are already at capacity with new ones
        # we don't want to decrement capacity
        if (leftovers):
            decrement_capacity = False
        break_loop = True
        instances_to_terminate = old_instances
        desired_size = min_size
        log.debug("No new instances needed")

    if (num_new_inst_needed < batch_size and num_new_inst_needed != 0):
        instances_to_terminate = instances_to_terminate[:num_new_inst_needed]
        decrement_capacity = False
        break_loop = False
        log.debug("{0} new instances needed".format(num_new_inst_needed))

    log.debug("decrementing capacity: {0}".format(decrement_capacity))

    for instance in instances_to_terminate:
        elb_dreg(asg_connection, elb_connection, elb2_connection, module, group_name, instance['InstanceId'])
        log.debug("terminating instance: {0}".format(instance['InstanceId']))
        asg_connection.terminate_instance_in_auto_scaling_group(InstanceId=instance['InstanceId'],
                                                                ShouldDecrementDesiredCapacity=decrement_capacity)

    # we wait to make sure the machines we marked as Unhealthy are
    # no longer in the list

    return break_loop, desired_size, instances_to_terminate


def wait_for_term_inst(asg_connection, module, term_instances):
    wait_timeout = module.params.get('wait_timeout')
    group_name = module.params.get('name')
    count = 1
    wait_timeout = time.time() + wait_timeout
    while (wait_timeout > time.time() and count > 0):
        log.debug("waiting for instances to terminate")
        count = 0
        asg = get_asg_by_name(asg_connection, group_name)
        instances = []
        for asg_instance in asg['Instances']:
            for term_instance in term_instances:
                if (asg_instance['InstanceId'] == term_instance['InstanceId']):
                    instances.append(asg_instance)
                    break

        for i in instances:
            lifecycle = i['LifecycleState']
            health = i['HealthStatus']
            log.debug("Instance {0} has state of {1},{2}".format(i['InstanceId'], lifecycle, health))
            if (lifecycle == 'Terminating' or health == 'Unhealthy'):
                count += 1
        time.sleep(10)

    if (wait_timeout <= time.time()):
        # waiting took too long
        module.fail_json(msg="Waited too long for old instances to terminate. %s" % time.asctime())


def wait_for_new_inst(module, asg_connection, group_name, wait_timeout, desired_size):
    # make sure we have the latest stats after that last loop.
    asg = get_asg_by_name(asg_connection, group_name)

    viable_instances = 0
    for instance in asg['Instances']:
        if (instance['HealthStatus'] == 'Healthy' and instance['LifecycleState'] == 'InService'):
            viable_instances += 1
    log.debug("Waiting for viable_instances = {0}, currently {1}".format(desired_size, viable_instances))

    # now we make sure that we have enough instances in a viable state
    wait_timeout = time.time() + wait_timeout
    while (wait_timeout > time.time() and desired_size > viable_instances):
        log.debug("Waiting for viable_instances = {0}, currently {1}".format(desired_size, viable_instances))
        time.sleep(10)
        asg = get_asg_by_name(asg_connection, group_name)
        viable_instances = 0
        for instance in asg['Instances']:
            if (instance['HealthStatus'] == 'Healthy' and instance['LifecycleState'] == 'InService'):
                viable_instances += 1

    if (wait_timeout <= time.time()):
        # waiting took too long
        module.fail_json(msg="Waited too long for new instances to become viable. %s" % time.asctime())
    log.debug("Reached viable_instances: {0}".format(desired_size))
    return asg


def main():
    argument_spec = ec2_argument_spec()
    argument_spec.update(
        dict(
            name=dict(required=True, type='str'),
            load_balancers=dict(type='list'),
            target_groups=dict(type='list'),
            availability_zones=dict(type='list'),
            launch_config_name=dict(type='str'),
            min_size=dict(type='int'),
            max_size=dict(type='int'),
            desired_capacity=dict(type='int'),
            vpc_zone_identifier=dict(type='list'),
            replace_batch_size=dict(type='int', default=1),
            replace_all_instances=dict(type='bool', default=False),
            replace_instances=dict(type='list', default=[]),
            lc_check=dict(type='bool', default=True),
            wait_timeout=dict(type='int', default=300),
            state=dict(default='present', choices=['present', 'absent']),
            tags=dict(type='list'),
            health_check_period=dict(type='int', default=300),
            health_check_type=dict(default='EC2', choices=['EC2', 'ELB']),
            default_cooldown=dict(type='int', default=300),
            wait_for_instances=dict(type='bool', default=True),
            termination_policies=dict(type='list', default='Default'),
            notification_topic=dict(type='str', default=None),
            notification_types=dict(type='list', default=[
                'autoscaling:EC2_INSTANCE_LAUNCH',
                'autoscaling:EC2_INSTANCE_LAUNCH_ERROR',
                'autoscaling:EC2_INSTANCE_TERMINATE',
                'autoscaling:EC2_INSTANCE_TERMINATE_ERROR'
            ])
        ),
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        mutually_exclusive=[['replace_all_instances', 'replace_instances']]
    )

    if (not HAS_BOTO):
        module.fail_json(msg='boto3 required for this module')

    state = module.params.get('state')
    replace_instances = module.params.get('replace_instances')
    replace_all_instances = module.params.get('replace_all_instances')
    region, ec2_url, aws_connect_params = get_aws_connection_info(module, boto3=True)
    try:
        asg_connection = boto3_conn(module, conn_type="client", resource="autoscaling", region=region, endpoint=ec2_url,
                                    **aws_connect_params)
        elb_connection = boto3_conn(module, conn_type="client", resource="elb", region=region, endpoint=ec2_url,
                                    **aws_connect_params)
        elb2_connection = boto3_conn(module, conn_type="client", resource="elbv2", region=region, endpoint=ec2_url,
                                     **aws_connect_params)
        ec2_connection = boto3_conn(module, conn_type="client", resource="ec2", region=region, endpoint=ec2_url,
                                    **aws_connect_params)
        if (not asg_connection):
            module.fail_json(msg="failed to connect to AWS for the given region: %s" % str(region))
    except botocore.exceptions.ClientError as e:
        module.fail_json(msg=str(e))
    changed = create_changed = replace_changed = False

    if (state == 'present'):
        create_changed, asg_properties = create_autoscaling_group(asg_connection, ec2_connection, elb_connection,
                                                                  elb2_connection, module)
    elif (state == 'absent'):
        changed = delete_autoscaling_group(asg_connection, module)
        module.exit_json(changed=changed)
    if (replace_all_instances or replace_instances):
        replace_changed, asg_properties = replace(asg_connection, elb_connection, elb2_connection, module)
    if (create_changed or replace_changed):
        changed = True
    module.exit_json(changed=changed, **asg_properties)


if (__name__ == '__main__'):
    main()
