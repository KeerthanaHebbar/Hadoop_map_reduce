import threading 
from collections import defaultdict
from queue import Queue

with open('input.txt','r') as f:
    document=f.read()
    
def map_fn(chunk,output_queue):
    counts=defaultdict(int)
    for word in chunk.split():
        counts[word]+=1
    output_queue.put(list(counts.items()))

def shuffle_fn(output_list):
    shuffled=defaultdict(list)
    for item in output_list:
        key,value=item
        shuffled[key].append(value)
    return list(shuffled.items())

def reduce_fn(item,output_queue):
    key, values=item
    output_queue.put((key,sum(values)))

num_threads=4
chunk_size=len(document)//num_threads
chunk=[document[i:i+chunk_size] for i in range(0,len(document),chunk_size)]

output_queue=Queue()
threads=[]
for i in range(num_threads):
    t=threading.Thread(target=map_fn, args=(chunk[i], output_queue))
    threads.append(t)
    

for t in threads:
    t.start()
for t in threads:
    t.join()

output_list=[]
while not output_queue.empty():
    output_list.extend(output_queue.get())
    
print("Intermediate outputs from map:")
for item in output_list:
    print(item)
    
shuffled_list=shuffle_fn(output_list)

output_queue=Queue()
threads=[]
for i in range(num_threads):
    t=threading.Thread(target=reduce_fn, args=(shuffled_list[i],output_queue))
    threads.append(t)
    

for t in threads:
    t.start()
for t in threads:
    t.join()

final_output=[]
while not output_queue.empty():
    final_output.append(output_queue.get())
    

result=defaultdict(int)
for item in final_output:
    key, value=item
    result[key]+=value 
    

print("The final word count:")    
for key,value in result.items():
    print(key,value)
    
    
