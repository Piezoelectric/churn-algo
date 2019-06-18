import pprint

#===============================================================================
#LOG SCRAPING CODE
#Scrapes data from log into dict format
#===============================================================================

filename = "C:\\Users\\Gerald\\OneDrive\\Documents\\school\\TAMU\\2018Fall\\rsrsch\\2019-06-18-churn-log.txt"
logHandle = open(filename, 'r')
print("Filename:", filename)

operations = {} #Stores all reads and writes, indexed by r/w value
#value: [target node, operation type, start time, end time]

unfinishedOperations = {}
#Stores operations that haven't finished,
#indexed by node ID
#(Each node can only have one operation pending on it at a time,
#so when a node finishes an operation and generates a response,
#there is only one invocation that response can match to.
#so it's safe to index by node ID)

beginTime = None #Stores when an operation begins
simulationBeginTime = None #Stores when the entire simulation begins

for line in logHandle:
    #Discard the first few lines (which are only for calibrating DELAY).
    #Calibrating DELAY involves 2 extra reads which are not part of the
    #algorithm's actual execution
    if "main loop" in line:
        break #Exit this loop once the scheduler begins scheduling operations for the algorithm

for line in logHandle:
    if "time.time()" in line: #Operation begin time
        tokens = line.split()
        beginTime = tokens[-1]

        if "Iteration 0" in line: #The whole simulation began at this time
            simulationBeginTime = beginTime
        
    elif "target:" in line: #Nature of the operation
        tokens = line.split()
        target = tokens[-1]
        operation = tokens[6] #or indx -8
        writeValue = tokens[9] #or indx -5
        
        #If it's read or write, we add it to unfinishedOperations
        #and note that it's begun (but not necessarily ended)
        #otherwise don't bother
        #format: [operation, beginTime, endTime, read/writeVal]
        if operation == "read":
            unfinishedOperations[target] = ["read", beginTime, None, None]
            
        elif operation == "write":
            unfinishedOperations[target] = ["write", beginTime, None, writeValue]
        
    elif "returns with" in line: #A read or write has finished
        #Match operation start with operation finish via node IDs
        tokens = line.split()
        target = tokens[5]
        operation = tokens[2]
        readValue = tokens[9]
        endTime = tokens[-1]
        
        try:
            if operation == "read":
                unfinishedOperations[target][2] = endTime
                unfinishedOperations[target][3] = readValue
            elif operation == "write":
                unfinishedOperations[target][2] = endTime
            else:
                print(operation)

            #Add finished operation to list; reset unfinishedOperations
            if operation == "read" or operation == "write":
                value = unfinishedOperations[target][-1]
                finishedOperation = [target] + unfinishedOperations[target][:-1]
                if value not in operations.keys():
                    operations[value] = []
                operations[value].append(finishedOperation)
                unfinishedOperations[target] = []
        except KeyError:
            print("Couldn't match this response to any invocation",line)


#None is just the value the shared mem is initialized to.
#(Could define "write None" as starting when the sim starts,
#and finishing immediately)
#We do need to keep None around, in case write(2) finishes and
#afterwards some node starts reading and reads None 
if "None" in operations.keys():
    operations["None"].append(["Initial Value", "write",
                               simulationBeginTime, simulationBeginTime])

pp = pprint.PrettyPrinter(indent=4)
print("Unfinished Operations:")
pp.pprint(unfinishedOperations)
print("\n\nOperations:")
pp.pprint(operations)

#===============================================================================
#LINEARIZABILITY CHECKER
#Implements Gibbons-Korach (1997) Theorem 4.2 to check for VL
#with read mappings
#===============================================================================

#As a reminder, operations() is a dict
#storing all reads and writes, indexed by r/w value
#value: [target node, operation type, start time, end time]

forwardZones = []
backwardZones = []
#A zone for a particular r/w value is defined by
#(earliest end-of-interval time) to (latest start-of-interval time)
#We record it as [value, startTime, endTime]

#Iterate over each value that ended up in the shared register,
#Check each cluster, and if the cluster's ok, add the cluster's zone
#to one of the above zone lists
for value in list(operations.keys()):
    print("Checking value", value)
    #Filter for the write operation (and check that there's only one)
    writeOperations = [x for x in operations[value] if x[1] == "write"]
    if len(writeOperations) > 1:
        print("Wrote", value, "more than once, read-mapping is broken")
    elif len(writeOperations) == 0 and value != "None": 
        print(value, "was never written but somehow read") #negative instance

    writeOperation = writeOperations[0]
    writeBeginTime = float(writeOperation[2])

    #Filter for all read operations
    readOperations = [x for x in operations[value] if x[1] == "read"]
    readEndTimes = [float(x[3]) for x in readOperations]

    #Step 1: Start-of-interval time for write must be earlier than
    #end-of-interval time for every read
    check1 = all(time >= writeBeginTime for time in readEndTimes)
    print("Check 1", check1)
    
    #Step 2: Define the zone for this cluster
    earliestEndTime = min([x[3] for x in operations[value]])
    latestStartTime = max([x[2] for x in operations[value]])
    print("Earliest end time", earliestEndTime,
          "Latest start time", latestStartTime)

    if earliestEndTime < latestStartTime:
        print("Cluster of value", value, "is a Forward zone\n")
        forwardZones.append( [value, earliestEndTime, latestStartTime] )
    else:
        print("Cluster of value", value, "is a Backward zone\n")
        backwardZones.append( [value, latestStartTime, earliestEndTime] )

print("Forward zones")
pp.pprint(forwardZones)
print("Backward zones")
pp.pprint(backwardZones)

#We record a zone as [value, startTime, endTime]

#Check if two forward zones overlap
#Sort by start time,
#then check that each entry's end time
#is after the previous start time
forwardZones.sort(key=lambda x: x[1])


for index in range(1,len(forwardZones)):
    zone = forwardZones[index]
    previousForwardZone = forwardZones[index-1]
    if zone[2] < previousForwardZone[1]:
        print("This zone overlaps with previous zone")
        print(zone, previousForwardZone)
    else:
        print("Forward zone is OK")


#Check if a backward zone is contained within a forward zone
for backwardZone in backwardZones:
    for forwardZone in forwardZones:
        if forwardZone[1] < backwardZone[1] and backwardZone[2] < forwardZone[2]:
            print("Backward zone contained in forward zone")
            print(backwardZone, forwardZone)
        else:
            print("Backward zone is OK")

