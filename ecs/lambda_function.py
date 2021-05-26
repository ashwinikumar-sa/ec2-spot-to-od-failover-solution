
import boto3
import json
import logging
import os
import time

ecsclient = boto3.client('ecs')
ssmclient = boto3.client('ssm')
simulateTaskPlacementFailure = False
failoverServiceName=os.getenv('FARGATE_FAILOVER_SERVICE_NAME')

def ECSTaskPlacementHandler(clusterName, serviceName, eventType):
    
    print("ECSTaskPlacementHandler handling {} event for clusterName {} serviceName{}".format(eventType, clusterName, serviceName))
    
    status = False
    
    try:

        res, missingCount =   getMissingCount(clusterName, serviceName)
        if res == False:
            return status
            
        if eventType == "SERVICE_TASK_PLACEMENT_FAILURE":
            
            #print("simulateTaskPlacementFailure={}".format(simulateTaskPlacementFailure))
            if simulateTaskPlacementFailure == True:
                missingCount += 1
                print("Increasing missingCount since simulateTaskPlacementFailure is True. New missingCount={}".format(missingCount))
                            
            res = failoverToOrFromOnDemandService(clusterName, failoverServiceName, missingCount)
            if res == True:
                setSSMFlag(clusterName, serviceName, "YES")
                status = True
            else:
                print("failoverToOrFromOnDemandService failed.")
            
            
        elif eventType == "ECS Task State Change":

            paramValue = getSSMFlag(clusterName, serviceName)
            
            if paramValue == "YES":
                
                res = failoverToOrFromOnDemandService(clusterName, failoverServiceName, missingCount)
                if res == True:
                    if missingCount == 0:
                        setSSMFlag(clusterName, serviceName, "NO")                    
                    status = True
                else:
                    print("failoverToOrFromOnDemandService failed.")

            else:
                print("handleTaskStateChange: No action taken since paramValue={} for clusterName={} serviceName={}".format(paramValue, clusterName, serviceName))

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
            
    #print("isCapacityIssuesWithFargate={} isCapacityIssuesWithFargateSpot={}".format(isCapacityIssuesWithFargate, isCapacityIssuesWithFargateSpot))
    return isCapacityIssuesWithFargateSpot
    
    
def test():
    
    print("write test code here")
    

def lambda_handler(event, context):
    #logger.debug('Event received %s ' % event)

    global simulateTaskPlacementFailure
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
                #print("Received event SERVICE_TASK_PLACEMENT_FAILURE for FARGATE_SPOT")
                
                clusterName = event['resources'][0].split('/')[1]
                serviceName = event['resources'][0].split('/')[2]
                
                #print("keys={}".format(event.keys()))
                if "simulateTaskPlacementFailure" in event.keys():
                    if event['simulateTaskPlacementFailure'] == "True":
                        print("setting")
                        simulateTaskPlacementFailure = True
                        print(simulateTaskPlacementFailure)
            
               
                #print("clusterName={} serviceName={}".format(clusterName, serviceName))
                
                res = ECSTaskPlacementHandler(clusterName, serviceName, "SERVICE_TASK_PLACEMENT_FAILURE")
                
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
            
            #print("clusterName={} serviceName={}".format(clusterName, serviceName))
                
            res = ECSTaskPlacementHandler(clusterName, serviceName, "ECS Task State Change")
                
            if res == False:
                print("ECSTaskPlacementHandler is ignored since current deployment is not yet completed")
                    
                    
        else:
            print("Cannot handle event state {} for CP {}for now".format(event['detail']['lastStatus'], event['detail']['capacityProviderName']))
            
    return {
        'statusCode': 200,
        'body': json.dumps("Handled event {} successfully".format(event['detail-type']))
    }