"""
Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
the License. A copy of the License is located at

     https://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
and limitations under the License. """
from __future__ import print_function

import base64
import logging
import os
import re
import uuid

import boto3
from boto3.dynamodb.conditions import Key
from boto3.exceptions import ResourceLoadException

logger = logging.getLogger()
logger.setLevel(logging.INFO)
import requests

url = "https://dnfqbtu4va.execute-api.cn-north-1.amazonaws.com.cn/dev/" \
      "query?database=cn_cdp_dev&schema=analytical_appddm_mob&table=mob_activity_kpi"

payload = {}
headers = {
  'x-api-key': 'jY2pDEjCR0azvOMOOa4oc8ISHdvzQDQ18hqraVix',
  'Authorization': 'Basic dGVzdF91c2VybmFtZTp0ZXN0X3Bhc3N3b3Jk'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)


def lambda_handler(event, context):
    """Do not print the auth token unless absolutely necessary """
    token = None
    if event['type'] == "TOKEN":
        token = event['authorizationToken']
    elif event['type'] == "REQUEST":
        token = event['headers']['Authorization']
    else:
        logger.debug(event)
        logger.debug(context)

    # logger.debug(f"Client token: {token}")
    logger.info(f"Method ARN: {event['methodArn']}")

    principal_id = f"{uuid.uuid4().hex}"

    """if the token is valid, a policy must be generated which will allow or deny access to the client"""
    """if access is denied, the client will receive a 403 Access Denied response"""
    """if access is allowed, 
    API Gateway will proceed with the backend integration configured on the method that was called """

    tmp = event['methodArn'].split(':')
    api_gateway_arn_tmp = tmp[5].split('/')
    aws_account_id = tmp[4]

    secret_name = verify_token_from_dynamo_db(token)

    policy = AuthPolicy(principal_id, aws_account_id)
    policy.rest_api_id = api_gateway_arn_tmp[0]
    policy.partition = tmp[1]
    policy.region = tmp[3]
    policy.resource = api_gateway_arn_tmp[3]
    policy.method = api_gateway_arn_tmp[2]
    policy.stage = api_gateway_arn_tmp[1]
    if secret_name is not None:
        # policy.allow_all_methods()
        policy.allow_method(getattr(HttpVerb, policy.method), policy.resource)
        # policy.allow_method(HttpVerb.GET, "query")
        logger.info(f"Token verified, allowing resourceARN: {policy.allow_methods[0]['resourceArn']}")
    else:
        policy.deny_all_methods()
        logger.info("Token verify failed, denying all methods.")

    # Finally, build the policy
    auth_response = policy.build()

    # new! -- add additional key-value pairs associated with the authenticated principal
    # these are made available by APIGW like so: $context.authorizer.<key>
    # additional context is cached
    return_context = {
        'secret_name': secret_name,  # $context.authorizer.key -> value
        # 'number': 1,
        # 'bool': True
    }
    # context['arr'] = ['foo'] <- this is invalid, APIGW will not accept it
    # context['obj'] = {'foo':'bar'} <- also invalid

    auth_response['context'] = return_context

    return auth_response


class HttpVerb:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    HEAD = "HEAD"
    DELETE = "DELETE"
    OPTIONS = "OPTIONS"
    ANY = "ANY"
    ALL = "*"


def verify_token_from_dynamo_db(token):
    # logger.debug(os.environ)
    try:
        credential = base64.b64decode(token.split(' ')[1]).decode("utf-8")
    except (ValueError, IndexError) as e:
        logger.warning(f"Error occurred while decoding base64-encoded token, message is: {e}")
        return None
    if ":" in credential:
        username = credential.split(":")[0]
        password = credential.split(":")[1]
    else:
        logger.warning("The Token provided is not in correct format.")
        return None
    runtime_region = os.environ.get('AWS_REGION')
    authorize_table = os.environ.get('authorize_table')
    dynamodb_resource = boto3.resource(
        service_name='dynamodb',
        region_name=runtime_region,
    )
    table_config = dynamodb_resource.Table(authorize_table)
    key_condition = Key("Entity_Key").eq("Authenticate_Key")
    filter_condition = Key("Authenticate_Username").eq(username) & Key("Authenticate_Password").eq(password)
    response_config = table_config.query(
        KeyConditionExpression=key_condition,
        FilterExpression=filter_condition,
        ProjectionExpression='Secret_Name',
    )
    if response_config["ResponseMetadata"]["HTTPStatusCode"] != 200:
        logger.error(msg=response_config["ResponseMetadata"])
        raise ResourceLoadException(f'DynamoDB returns error status code: '
                                    f'{response_config["ResponseMetadata"]["HTTPStatusCode"]}')
    if response_config['Count'] == 1:
        secret_name = response_config['Items'][0]['Secret_Name']
        logger.info("secret name retrieved")
        logger.debug(secret_name)
    else:
        logger.warning("No match found for this Token.")
        return None
    return secret_name


class AuthPolicy(object):
    aws_account_id = ""
    """The AWS account id the policy will be generated for. This is used to create the method ARNs."""
    principal_id = ""
    """The principal used for the policy, this should be a unique identifier for the end user."""
    version = "2012-10-17"
    """The policy version used for the evaluation. This should always be '2012-10-17'"""
    path_regex = "^[/.a-zA-Z0-9-*]+$"
    """The regular expression used to validate resource paths for the policy"""

    """these are the internal lists of allowed and denied methods. These are lists
    of objects and each object has 2 properties: A resource ARN and a nullable
    conditions statement.
    the build method processes these lists and generates the appropriate
    statements for the final policy"""
    allow_methods = []
    deny_methods = []

    rest_api_id = "rest_api_id"

    region = "cn-north-1"

    stage = "test_stage"

    partition = "aws"

    resource = "test_resource"

    method = "ANY"

    def __init__(self, principal, aws_account_id):
        self.aws_account_id = aws_account_id
        self.principal_id = principal
        self.allow_methods = []
        self.deny_methods = []

    def _add_method(self, effect, verb, resource, conditions):
        """Adds a method to the internal lists of allowed or denied methods. Each object in
        the internal list contains a resource ARN and a condition statement. The condition
        statement can be null."""
        if verb != "*" and not hasattr(HttpVerb, verb):
            raise NameError("Invalid HTTP verb " + verb + ". Allowed verbs in HttpVerb class")
        resource_pattern = re.compile(self.path_regex)
        if not resource_pattern.match(resource):
            raise NameError("Invalid resource path: " + resource + ". Path should match " + self.path_regex)

        if resource[:1] == "/":
            resource = resource[1:]

        resource_arn = f"arn:{self.partition}:execute-api:" \
                       f"{self.region}:{self.aws_account_id}:" \
                       f"{self.rest_api_id}/{self.stage}/{verb}/{resource}"

        if effect.lower() == "allow":
            self.allow_methods.append({
                'resourceArn': resource_arn,
                'conditions': conditions
            })
        elif effect.lower() == "deny":
            self.deny_methods.append({
                'resourceArn': resource_arn,
                'conditions': conditions
            })

    @staticmethod
    def _get_empty_statement(effect):
        """Returns an empty statement object prepopulated with the correct action and the desired effect."""
        statement = {
            'Action': 'execute-api:Invoke',
            'Effect': effect[:1].upper() + effect[1:].lower(),
            'Resource': []
        }

        return statement

    def _get_statement_for_effect(self, effect, methods):
        """This function loops over an array of objects containing a resourceArn and
        conditions statement and generates the array of statements for the policy."""
        statements = []

        if len(methods) > 0:
            statement = self._get_empty_statement(effect)

            for cur_method in methods:
                if cur_method['conditions'] is None or len(cur_method['conditions']) == 0:
                    statement['Resource'].append(cur_method['resourceArn'])
                else:
                    conditional_statement = self._get_empty_statement(effect)
                    conditional_statement['Resource'].append(cur_method['resourceArn'])
                    conditional_statement['Condition'] = cur_method['conditions']
                    statements.append(conditional_statement)

            statements.append(statement)

        return statements

    def allow_all_methods(self):
        """Adds a '*' allow to the policy to authorize access to all methods of an API"""
        self._add_method("Allow", HttpVerb.ALL, "*", [])

    def deny_all_methods(self):
        """Adds a '*' allow to the policy to deny access to all methods of an API"""
        self._add_method("Deny", HttpVerb.ALL, "*", [])

    def allow_method(self, verb, resource):
        """Adds an API Gateway method (Http verb + Resource path) to the list of allowed
        methods for the policy"""
        self._add_method("Allow", verb, resource, [])

    def deny_method(self, verb, resource):
        """Adds an API Gateway method (Http verb + Resource path) to the list of denied
        methods for the policy"""
        self._add_method("Deny", verb, resource, [])

    def allow_method_with_conditions(self, verb, resource, conditions):
        """Adds an API Gateway method (Http verb + Resource path) to the list of allowed
        methods and includes a condition for the policy statement. More on AWS policy
        conditions here: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements.html#Condition"""
        self._add_method("Allow", verb, resource, conditions)

    def deny_method_with_conditions(self, verb, resource, conditions):
        """Adds an API Gateway method (Http verb + Resource path) to the list of denied
        methods and includes a condition for the policy statement. More on AWS policy
        conditions here: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements.html#Condition"""
        self._add_method("Deny", verb, resource, conditions)

    def build(self):
        """Generates the policy document based on the internal lists of allowed and denied
        conditions. This will generate a policy with two main statements for the effect:
        one statement for Allow and one statement for Deny.
        Methods that includes conditions will have their own statement in the policy."""
        if ((self.allow_methods is None or len(self.allow_methods) == 0) and
                (self.deny_methods is None or len(self.deny_methods) == 0)):
            raise NameError("No statements defined for the policy")

        policy = {
            'principalId': self.principal_id,
            'policyDocument': {
                'Version': self.version,
                'Statement': []
            }
        }

        policy['policyDocument']['Statement'].extend(self._get_statement_for_effect("Allow", self.allow_methods))
        policy['policyDocument']['Statement'].extend(self._get_statement_for_effect("Deny", self.deny_methods))

        return policy
