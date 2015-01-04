import json

filteredUsrFile = open("yelp_filtered_graph_foodie.txt", 'w')

foodieList = []
with open("./yelp_restaurant_lovers_list.txt") as foodieFile:
    for line in foodieFile:
        a = line.split('\t')
        foodieList.append(a[0])
foodieSet = set(foodieList)
        
with open("./yelp_academic_dataset_user.json") as userFile:
    for line in userFile:
        d = json.loads(line)
        if d['user_id'] in foodieSet:
            if d['friends']:
               frnds = []
               
               for dost in d['friends']:
                   if dost in foodieSet:
                       frnds.append(dost)

               if frnds:
                   filteredUsrFile.write(d['user_id']+':'+' '.join(frnds)+'\n')

