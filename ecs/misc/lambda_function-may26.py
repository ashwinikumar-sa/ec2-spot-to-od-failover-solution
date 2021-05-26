
import boto3
import json
import logging
import os
import time

ecsclient = boto3.client('ecs')
ssmclient = boto3.client('ssm')
simulateTaskPlacementFailure = False
failoverServiceName=os.getenv('FARGATE_FAILOVER_SERVICE_NAME')

def handleTaskPlacementFailure(clusterName, serviceName):
    
    print("handleTaskPlacementFailure")
    status = False
    
    try:

        res, missingCount =   getMissingCount(clusterName, serviceName)
        if res == False:
            return status

        status = failoverToOrFromOnDemandService(clusterName, failoverServiceName, missingCount)
        if status == True:
            setSSMFlag(clusterName, serviceName, "YES")
        else:
            print("failoverToOrFromOnDemandService failed.")
        
    except Exception as e:
        print("error in handleTaskPlacementFailure:{}".format(str(e)))
    
    
    return status



def getMissingCount(clusterName, serviceName):
    
    status = False
    missingCount = 0
    
    try:
        res = ecsclient.describe_services(
            cluster=clusterName,
            services=[
                serviceName,
            ],
        )
            
        desiredCount = res['services'][0]['desiredCount']
        pendingCount = res['services'][0]['pendingCount']
        runningCount = res['services'][0]['runningCount']
        
        if desiredCount >= runningCount:
            missingCount = desiredCount -  runningCount
        else:
            missingCount = 0
        
        print("getMissingCount serviceName={} desiredCount={} runningCount={} pendingCount={} missingCount={}".format(serviceName, desiredCount, runningCount, pendingCount, missingCount))

        if simulateTaskPlacementFailure == True:
            missingCount += 1
            print("Increasing missingCount since simulateTaskPlacementFailure is True. New missingCount={}".format(missingCount))
            
        status = True

    except Exception as e:
        print("error in getMissingCount:{}".format(str(e)))
    
    return status, missingCount

def setSSMFlag(clusterName, serviceName, paramValue):
    
    try:    
        paramName = '/'+clusterName+'/'+serviceName
        
        print("setSSMFlag paramName={} paramValue={}".format(paramName, paramValue))
        
        ssmclient.put_parameter(
          Name = paramName,
          Value = paramValue,
          Type = 'String',
          Overwrite = True
        )
    except Exception as e:
        print("error in setSSMFlag:{}".format(str(e)))
   
def getSSMFlag(clusterName, serviceName):
    
    try:    
        paramName = '/'+clusterName+'/'+serviceName
        resp = ssmclient.get_parameter(Name=paramName)
        paramValue = resp['Parameter']['Value']
        print("getSSMFlag paramName={} paramValue={}".format(paramName, paramValue))
        
        return paramValue

    except Exception as e:
        print("error in getSSMFlag:{}".format(str(e)))
   
      

def failoverToOrFromOnDemandService(clusterName, failoverServiceName, missingCount):

    status = False
    
    try:
        
        res = ecsclient.describe_services(
            cluster=clusterName,
            services=[
                failoverServiceName,
            ],
        )
        
        desiredCount = res['services'][0]['desiredCount']
        pendingCount = res['services'][0]['pendingCount']
        runningCount = res['services'][0]['runningCount']
        
        print("failoverToOrFromOnDemandService failoverServiceName={} desiredCount={} runningCount={} pendingCount={} NEW desiredCount={}".format(failoverServiceName, desiredCount, runningCount, pendingCount, missingCount))
        
        res = ecsclient.update_service(
            cluster = clusterName,
            service = failoverServiceName,
            desiredCount=missingCount,
        )
        
        status = True

    except Exception as e:
        print(e)

    return status
    

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
    
        
def handleTaskStateChange(clusterName, serviceName):
    print("clusterName={} serviceName={}".format(clusterName, serviceName))
    
    paramValue = getSSMFlag(clusterName, serviceName)
    
    if paramValue == "YES":
        missingCount = getMissingCount(clusterName, serviceName)
        failoverToOrFromOnDemandService(clusterName, failoverServiceName, missingCount)
        if missingCount == 0:
            setSSMFlag(clusterName, serviceName, "NO")
    else:
        print("handleTaskStateChange: No action taken since paramValue={} for clusterName={} serviceName={}".format(paramValue, clusterName, serviceName))
    
    
    
    
def lambda_handler(event, context):
    #logger.debug('Event received %s ' % event)

    #test()
    #return {
    #    'statusCode': 200,
    #    'body': json.dumps('Hello from Lambda!')
    #}    
    
    
    #print(event)
    
    if event['detail-type'] == 'ECS Service Action' :
        
        if event['detail']['eventName'] == 'SERVICE_TASK_PLACEMENT_FAILURE' and event['detail']['reason'] == 'RESOURCE:FARGATE':
            res = checkIfCapacityIssueWithFargateSpot( event['detail']['capacityProviderArns'])
            
            if res == True:
                print("Received event SERVICE_TASK_PLACEMENT_FAILURE for FARGATE_SPOT")
                
                clusterName = event['resources'][0].split('/')[1]
                serviceName = event['resources'][0].split('/')[2]
                
                if "simulateTaskPlacementFailure" in event.keys():
                    if event['simulateTaskPlacementFailure'] == "True":
                        simulateTaskPlacementFailure = True
            
                print("clusterName={} serviceName={}".format(clusterName, serviceName))
                
                res = handleTaskPlacementFailure(clusterName, serviceName)
                
                if res == False:
                    print("SERVICE_TASK_PLACEMENT_FAILURE is ignored since current deployment is not yet completed")
                    
            else:
                print("There is no capacity issue with Fargate Spot. It may be due to Fargate which is ignored currently")
        
        else:
            print("Cannot handle event names {} for now".format(event['detail']['eventName']))
            
    elif event['detail-type'] == 'ECS Task State Change':
        
        if event['detail']['capacityProviderName'] == 'FARGATE_SPOT' and event['detail']['lastStatus'] == 'RUNNING':
            
            clusterName = event['detail']['clusterArn'].split('/')[1]
            serviceName = event['detail']['group'].split(':')[1]
            
            print("clusterName={} serviceName={}".format(clusterName, serviceName))
                
            res = handleTaskStateChange(clusterName, serviceName)
                
            if res == False:
                print("handleTaskStateChange is ignored since current deployment is not yet completed")
                    
                    
        else:
            print("Cannot handle event state {} for CP {}for now".format(event['detail']['lastStatus'], event['detail']['capacityProviderName']))
            
    
    
    return  ("cluster: %s service: %s  updated" %(clusterName,serviceName))
    
    #return {
    #    'statusCode': 200,
    #    'body': json.dumps('Hello from Lambda!')
    #}