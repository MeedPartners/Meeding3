import psycopg2

def runSQL(remark, inString):
    print (remark + ' via ' + inString)
    try:
      cur.execute(inString)
      cur.execute('commit')
    except psycopg2.Error as e:
      print ("Wow, not: " + e.pgerror)
      return 0
    else:
     return 1

def sq(inString):
  return chr(39)+inString+chr(39)

def add_extra_fields(target, thisMany, ofThese):
    print (' adding ' + str(thisMany) + ' '+ofThese)
    added = 1
    big = ''
    if ofThese == 'numeric':
      big = '(25,10)'
    if ofThese == 'varchar':
      big = '(255)'
    while added <= thisMany:
      target += 'x'+ofThese[0]+str(added) + ' ' + ofThese + ' ' + big + ', '
      added += 1
    return target

conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='superguy', database='TestDB1', sslmode='allow')
cur = conn.cursor()

#Set up Facts Table
runSQL('Dropping Facts', "Drop table mp_Facts")
runSQL('Creating Fact Table', "Create table mp_Facts (factName varchar(255), factValue varchar(255))")

stringSQL = 'Insert into mp_facts values ('+sq('sysPrefix')+','+sq('mp_')+')'
runSQL('Adding Values', stringSQL)

#Expedient - create objects
print (' ')
print ("Creating Metadata Definitions")
runSQL('Dropping Objects', "Drop table mp_Objects")
stringSQL = 'Create Table mp_Objects (objSeq integer, typeName varchar(64), numStrings integer, numNumbers integer, numDates integer, numBools integer)'
runSQL('Objects', stringSQL)

runSQL('Dropping Core Elements', "Drop table mp_Internal")
stringSQL = 'Create Table mp_Internal (objSeq Integer, coreSeq Integer,  name varchar(64), type varchar(32), size integer )'
runSQL('Objects Core Elements', stringSQL)

#Values for objects
print (' ')
print ("Defining Core Objects")
stringSQL = 'Insert into mp_Objects values (1,'+chr(39)+'Node'+ chr(39)+',20,20,10,10)'
runSQL('Adding Object '+chr(39)+'Node'+chr(39), stringSQL)
stringSQL = 'Insert into mp_Objects values (2,'+chr(39)+'Link'+ chr(39)+',10,10,5,5)'
runSQL('Adding Object '+chr(39)+'Link'+chr(39), stringSQL)
stringSQL = 'Insert into mp_Objects values (3,'+chr(39)+'Rule'+ chr(39)+',8,8,4,4)'
runSQL('Adding Object '+chr(39)+'Rule'+chr(39), stringSQL)
runSQL('Committing', 'Commit;')

#Core Elements  1 is Node
print (' ')
print ("Defining Core Object Elements: Node")
stringSQL = 'Insert into mp_Internal values (1,1, '+sq('NodeSeq')+','+sq('integer')+',0)'
runSQL('Node NodeSeq', stringSQL)
stringSQL = 'Insert into mp_Internal values (1,2, '+sq('Name')+','+sq('varchar')+',128)'
runSQL('Node Name', stringSQL)
stringSQL = 'Insert into mp_Internal values (1,3, '+sq('ID')+','+sq('varchar')+',128)'
runSQL('Node ID', stringSQL)
stringSQL = 'Insert into mp_Internal values (1,4, '+sq('Type')+','+sq('varchar')+',64)'
runSQL('Node Type', stringSQL)
stringSQL = 'Insert into mp_Internal values (1,5, '+sq('Description')+','+sq('varchar')+',512)'
runSQL('Node Desc', stringSQL)
stringSQL = 'Insert into mp_Internal values (1,6, '+sq('effStart')+','+sq('date')+',0)'
runSQL('Node effStart', stringSQL)
stringSQL = 'Insert into mp_Internal values (1,7, '+sq('effEnd')+','+sq('date')+',0)'
runSQL('Node effEnd', stringSQL)
runSQL('Committing', 'Commit;')

#Core Elements  2 is Link
print ("Defining Core Object Elements: Link")
stringSQL = 'Insert into mp_Internal values (2,8, '+sq('LinkSeq')+','+sq('integer')+',0)'
runSQL('Link LinkSeq', stringSQL)
stringSQL = 'Insert into mp_Internal values (2,9, '+sq('ParentSeq')+','+sq('integer')+',0)'
runSQL('Link ParentSeq', stringSQL)
stringSQL = 'Insert into mp_Internal values (2,10, '+sq('ChildSeq')+','+sq('integer')+',0)'
runSQL('Link ChildSeq', stringSQL)
stringSQL = 'Insert into mp_Internal values (2,11, '+sq('Type')+','+sq('varchar')+',64)'
runSQL('Link Type', stringSQL)
stringSQL = 'Insert into mp_Internal values (2,12, '+sq('effStart')+','+sq('date')+',0)'
runSQL('Link effStart', stringSQL)
stringSQL = 'Insert into mp_Internal values (2,13, '+sq('effEnd')+','+sq('date')+',0)'
runSQL('Link effEnd', stringSQL)
runSQL('Committing', 'Commit;')

#Core Elements  3 is Rule
print ("Defining Core Object Elements: Rule")
stringSQL = 'Insert into mp_Internal values (3,14, '+sq('RuleSeq')+','+sq('integer')+',0)'
runSQL('Rule LinkSeq', stringSQL)
stringSQL = 'Insert into mp_Internal values (3,15, '+sq('RuleText')+','+sq('varchar')+',2048)'
runSQL('Rule RuleText', stringSQL)
stringSQL = 'Insert into mp_Internal values (3,16, '+sq('Filter')+','+sq('varchar')+',1024)'
runSQL('Rule Filter', stringSQL)
stringSQL = 'Insert into mp_Internal values (3,17, '+sq('Type')+','+sq('varchar')+',64)'
runSQL('Rule Type', stringSQL)
stringSQL = 'Insert into mp_Internal values (3,18, '+sq('effStart')+','+sq('date')+',0)'
runSQL('Rule effStart', stringSQL)
stringSQL = 'Insert into mp_Internal values (3,19, '+sq('effEnd')+','+sq('date')+',0)'
runSQL('Rule effEnd', stringSQL)
runSQL('Committing', 'Commit;')


#Build core objects from Metadata -- first get object name and characteristics
print (' ')
print ("Now building core objects from metadata ")

# Leave cur for lookups and spot operations
cur2 = conn.cursor() # outer loop
cur3 = conn.cursor() # inner loop

cur2.execute('select * from mp_Objects')

for row in cur2:
  thisObjectSeq = row[0]
  thisObjectName = row[1]
  stringCnt = row[2]
  numberCnt = row[3]
  dateCnt = row[4]
  boolCnt = row[5]
  print ('Now acting on '+ str(thisObjectSeq) + ' ' + thisObjectName)
  tableName = 'mp_'+thisObjectName
  runSQL('Killing deprecated '+tableName, 'Drop table '+tableName)
  mondoStr = 'Create table '+tableName+ ' (  '

  stringSQL = 'select * from mp_Internal where objSeq = '+str(thisObjectSeq)
  cur3.execute(stringSQL)
  for row2 in cur3:
     if row2[4] == 0:
       mondoStr += row2[2]+' '+row2[3]
     else:
       mondoStr += row2[2]+' '+row2[3] + '(' +str(row2[4]) + ')'
     mondoStr += ', '

  #Now load up the empty containers
  mondoStr = add_extra_fields(mondoStr,stringCnt, 'varchar')
  mondoStr = add_extra_fields(mondoStr,numberCnt, 'numeric')
  mondoStr = add_extra_fields(mondoStr,stringCnt, 'integer')
  mondoStr = add_extra_fields(mondoStr,dateCnt, 'date')
  mondoStr = add_extra_fields(mondoStr,boolCnt, 'boolean')

  #Terminate the string, show and build
  mondoStr = mondoStr[:-2]+')'
  #print (mondoStr)
  runSQL('Building '+tableName,mondoStr)

cur.close()
cur2.close()
cur3.close()
conn.close()
