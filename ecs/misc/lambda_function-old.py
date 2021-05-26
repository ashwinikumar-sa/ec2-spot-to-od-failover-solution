
import boto3
import json
import logging
import os
import time


#logger = logging.getLogger('logger')
#logger.setLevel(logging.INFO)

ecsclient = boto3.client('ecs')
ssmclient = boto3.client('ssm')

failoverServiceName=os.getenv('FARGATE_FAILOVER_SERVICE_NAME')


#def log_error_message(e):
    #logger.error(e.response['Error']['Code'])
    #logger.error(e.response['Error']['Message'])
    
def handleTaskPlacementFailure(clusterName, serviceName):
    
    print("handleTaskPlacementFailure")
    status = False
    
    try:
        
        #arns = client.list_tasks(cluster=clusterName)['taskArns']
        #print(arns)
        
        #res = client.list_services(cluster=clusterName,maxResults=100)
        #print ("res={}".format(str(res)))
        
        res = ecsclient.describe_services(
            cluster=clusterName,
            services=[
                serviceName,
            ],
        )
        
        deployments = res['services'][0]['deployments']
        
        for dep in deployments:
            if dep['status'] == 'PRIMARY' and dep['rolloutState'] != "COMPLETED":
                print("Primary Deployment id : {} is still in state {}. So ignoring now".format(dep['id'], dep['rolloutState']))
                return True
            
        desiredCount = res['services'][0]['desiredCount']
        runningCount = res['services'][0]['runningCount']
        pendingCount = res['services'][0]['pendingCount']
        missingCount = desiredCount -  runningCount

        CPS = res['services'][0]['capacityProviderStrategy']
            
        fargate_base = fargate_spot_base = fargate_weight = fargate_spot_weight = 0
            
        for cp in CPS:
            if cp['capacityProvider'] == 'FARGATE':
                fargate_base= cp['base']
                fargate_weight= cp['weight']
            else:
                fargate_spot_base= cp['base']
                fargate_spot_weight= cp['weight']
            
        print("desiredCount={} runningCount={} pendingCount={} missingCount={}".format(desiredCount, runningCount, pendingCount, missingCount))
        print("CPS={}".format(CPS))
        print("fargate_base={} fargate_weight={} fargate_spot_base={} fargate_spot_weight={}".format(fargate_base, fargate_weight, fargate_spot_base, fargate_spot_weight))
        
        
        
        print("In a Steady State Service, missingCount will be zero. Increasing it by 1 to simulate the SERVICE_TASK_PLACEMENT_FAILURE event")
        print("Adding missingCount to the fargate_base and desiredCount for failover missing number of tasks to FARGATE")
        missingCount += 1 
        fargate_base += missingCount
        newDesiredCount =  desiredCount + missingCount
        print("missingCount={} fargate_base={} newDesiredCount={}".format(missingCount, fargate_base,newDesiredCount))

        newCPS = []
        
        newCPS.append(
                {
                    'capacityProvider': 'FARGATE',
                    'base': fargate_base,
                    'weight': fargate_weight
                },
            )

        newCPS.append(
                {
                    'capacityProvider': 'FARGATE_SPOT',
                    'base': fargate_spot_base,
                    'weight': fargate_spot_weight
                },
            )
            
        
        print("newCPS={}".format(newCPS))   
        
        reDeployServiceWithNewCapacityProviderStrategy(clusterName, serviceName, newCPS, newDesiredCount)
        
        paramName = clusterName+'-'+serviceName
        paramValue = "missingCount="+str(missingCount)+"desiredCount="+str(desiredCount)+"runningCount="+str(runningCount)
        
        print("Storing paramName={} paramValue={}".format(paramName, paramValue))        
        
        ssmclient.put_parameter(
          Name = paramName,
          Value = paramValue,
          Type = 'String',
          Overwrite = True
        )
        
        
  
        #logger.info('update service response %s ' % res)
        #print("res={}".format(res['services'][0]))
        return res
        
    except Exception as e:
        print("error in getMissingTasks:{}".format(str(e)))
        #log_error_message(e)    
    
    
    
    #print("response={}".format(response))


def reDeployServiceWithNewCapacityProviderStrategy(clusterName, serviceName, newCPS, newDesiredCount):

    try:
        
        response = ecsclient.update_service(
            cluster = clusterName,
            service = serviceName,
            desiredCount=newDesiredCount,
            capacityProviderStrategy=newCPS,
            forceNewDeployment=True,
        )
            
        waiter = ecsclient.get_waiter('services_stable')
        print("deployServices waiting for services {} to become stable in the ECS cluster {}".format(serviceName, clusterName))
        #waiter.wait( cluster=clusterName, services=[serviceName],  ) 
        #print("deployServices All the services {} are stable in the ECS cluster {}".format(serviceName, clusterName))
       
           
        #logger.debug('update service response %s ' % response)
    
    except Exception as e:
        print(e)
    #except ClientError as e:
        #log_error_message(e)
            
'''
"detail-type": "ECS Service Action",
  "source": "aws.ecs",
  "account": "000474600478",
  "time": "2019-11-19T19:55:38Z",
  "region": "us-west-2",
  "resources": [
    "arn:aws:ecs:us-east-1:000474600478:service/ecs-bluegreen-demo/fargate-service"
  ],
  "detail": {
    "eventType": "ERROR",
    "eventName": "SERVICE_TASK_PLACEMENT_FAILURE",
    "clusterArn": "arn:aws:ecs:us-east-1:000474600478:service/ecs-bluegreen-demo",
    "capacityProviderArns": [
      "arn:aws:ecs:us-east-1:000474600478:capacity-provider/FARGATE_SPOT"
    ],
    "reason": "RESOURCE:FARGATE",
    "createdAt": "2019-11-06T19:09:33.087Z"
  }
}
'''

def checkIfCapacityIssueWithFargateSpot(capacityProviderArns):
    
    isCapacityIssuesWithFargateSpot = False
    isCapacityIssuesWithFargate = False
    

    
    for arn in capacityProviderArns:
        print(arn)
        if 'FARGATE_SPOT' in arn:
            isCapacityIssuesWithFargateSpot = True
        elif 'FARGATET' in arn:
            isCapacityIssuesWithFargate = True
            
    print("isCapacityIssuesWithFargate={} isCapacityIssuesWithFargateSpot={}".format(isCapacityIssuesWithFargate, isCapacityIssuesWithFargateSpot))
    return isCapacityIssuesWithFargateSpot
    
    
def test():
    
    clusterName='ecs-bluegreen-demo'
    serviceName='fargate-service'
    desiredCount=4
    missingCount=1
    runningCount=3
    
    paramName = '/'+clusterName+'/'+serviceName
    paramValue = "missingCount="+str(missingCount)+",desiredCount="+str(desiredCount)+",runningCount="+str(runningCount)
    
    print("Storing paramName={} paramValue={}".format(paramName, paramValue))        
    
    ssmclient.put_parameter(
      Name = paramName,
      Value = paramValue,
      Type = 'String',
      Overwrite = True
    )
    
    resp = ssmclient.get_parameter(Name=paramName)
    
    print("resp = {}".format(resp['Parameter']['Value']))
    
        
def lambda_handler(event, context):
    #logger.debug('Event received %s ' % event)

    test()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }    
    
    
    #print(event)
    
    if event['detail-type'] == 'ECS Service Action' and event['detail']['eventName'] == 'SERVICE_TASK_PLACEMENT_FAILURE' and event['detail']['reason'] == 'RESOURCE:FARGATE':
        
        res = checkIfCapacityIssueWithFargateSpot( event['detail']['capacityProviderArns'])
        
        if res == True:
            print("Received event SERVICE_TASK_PLACEMENT_FAILURE for FARGATE_SPOT")
            
            clusterName = event['resources'][0].split('/')[1]
            serviceName = event['resources'][0].split('/')[2]
        
            print("clusterName={} serviceName={}".format(clusterName, serviceName))
            
            res = handleTaskPlacementFailure(clusterName, serviceName)
            
            if res == False:
                print("SERVICE_TASK_PLACEMENT_FAILURE is ignored since current deployment is not yet completed")
                
                

                        
            
        else:
            print("There is no capacity issue with Fargate Spot. It may be due to Fargate which is ignored currently")
        
        

    
    
    return  ("cluster: %s service: %s  updated" %(clusterName,serviceName))
    
    #return {
    #    'statusCode': 200,
    #    'body': json.dumps('Hello from Lambda!')
    #}