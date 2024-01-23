import os
from os import path, system
import random
import time

BFQ = "bfq"
KYBER = "kyber"
MQ_DEADLINE = "mq-deadline"
NONE = "none"



def set_scheduler(scheduler_):
    system('echo %s | sudo tee /sys/block/nvme0n1/queue/scheduler' % scheduler_)
    system('cat /sys/block/nvme0n1/queue/scheduler')
    
    print("I have choosen " + scheduler_)

def select_scheduler(operation, file_size, rec_length):
    scheduler = ""
    if (operation == "write"):
        if (rec_length == 8):
            if (file_size <= 1024*1024):
                scheduler = BFQ
            else:
                scheduler = NONE

        elif (rec_length == 64):
            if (file_size <= 4*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = NONE
        elif (rec_length == 512):
            if (file_size <= 16*1024*1024):
                scheduler = NONE
            else:
                scheduler = NONE
    elif (operation == "re-write"):
        if (rec_length == 8):
            if (file_size <= 1024*1024):
                scheduler = BFQ
            else:
                scheduler = BFQ

        elif (rec_length == 64):
            if (file_size <= 4*1024*1024):
                scheduler = KYBER
            else:
                scheduler = KYBER
        elif (rec_length == 512):
            if (file_size <= 16*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = NONE
    elif (operation == "read"):
        if (rec_length == 8):
            if (file_size <= 1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = NONE

        elif (rec_length == 64):
            if (file_size <= 4*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = KYBER
        elif (rec_length == 512):
            if (file_size <= 16*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = KYBER
    elif (operation == "re-read"):
        if (rec_length == 8):
            if (file_size <= 1024*1024):
                scheduler = KYBER
            else:
                scheduler = KYBER

        elif (rec_length == 64):
            if (file_size <= 4*1024*1024):
                scheduler = KYBER
            else:
                scheduler = KYBER
        elif (rec_length == 512):
            if (file_size <= 16*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = MQ_DEADLINE
    elif (operation == "random-write"):
        if (rec_length == 8):
            if (file_size <= 1024*1024):
                scheduler = BFQ
            else:
                scheduler = BFQ

        elif (rec_length == 64):
            if (file_size <= 4*1024*1024):
                scheduler = BFQ
            else:
                scheduler = KYBER
        elif (rec_length == 512):
            if (file_size <= 16*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = KYBER
    elif (operation == "random-read"):
        if (rec_length == 8):
            if (file_size <= 1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = KYBER

        elif (rec_length == 64):
            if (file_size <= 4*1024*1024):
                scheduler = KYBER
            else:
                scheduler = KYBER
        elif (rec_length == 512):
            if (file_size <= 16*1024*1024):
                scheduler = MQ_DEADLINE
            else:
                scheduler = MQ_DEADLINE
    set_scheduler(scheduler)

def create_test_file(file_path, file_size, record_length):
    chunk_size = 1024 * record_length  # Chunk size in bytes
    chunks = file_size // chunk_size  # Number of chunks

    with open(file_path, 'wb') as file:
        for _ in range(chunks):
            file.write(os.urandom(chunk_size))

def random_read_write(file_path, file_size, rec_length, operation_type):
    """Creates a test file with random write operations."""
    with open(file_path, 'w+b') as file:
        start_time = time.time()
        for _ in range(int(file_size/(rec_length*1024))):
            # Generate a random position within the file
            position = random.randint(0, int(file_size/(rec_length*1024)))
            position = position*rec_length
            # Seek to the random position and write the data
            file.seek(position)    
            if(operation_type=="random-write"):
                data = os.urandom(rec_length)
                file.write(data)
            elif(operation_type=="random-read"):
                file.read(rec_length)
        end_time = time.time()
        time_passed = end_time-start_time
        return time_passed

def sequential_read_write(file_path, file_size, rec_length, operation_type):
    """Performs sequential read or write operations on a file."""
    with open(file_path, 'w+b') as file:
        start_time = time.time()
        position = 0
        if operation_type == "read":
            data = os.urandom(rec_length)  # Preparing a single record of null bytes
            for _ in range(int(file_size/(rec_length*1024))):
                position += rec_length
                file.seek(position)
                file.write(data)
        elif operation_type == "write":
            for _ in range(int(file_size/(rec_length*1024))):
                position += rec_length
                file.seek(position)
                file.read(rec_length)
        end_time = time.time()
        time_passed = end_time - start_time
        return time_passed
                    
def call_function(file_path, file_size, record_length, operation_type):
    if operation_type == "random-write":
        time_passed = random_read_write(file_path, file_size, record_length, operation_type)
        print("Time passed for " + operation_type + " operation: " + str(time_passed))
    elif operation_type == "random-read":
        time_passed = random_read_write(file_path, file_size, record_length, operation_type)
        print("Time passed for " + operation_type + " operation: " + str(time_passed))
    elif operation_type == "read":
        time_passed = sequential_read_write(file_path, file_size, record_length, operation_type)
        print("Time passed for " + operation_type + " operation: " + str(time_passed))
    elif operation_type == "write":
        time_passed = sequential_read_write(file_path, file_size, record_length, operation_type)
        print("Time passed for " + operation_type + " operation: " + str(time_passed))
    elif operation_type == "re-read":
        sequential_read_write(file_path, file_size, record_length, "read")
        time_passed = sequential_read_write(file_path, file_size, record_length, "read")
        print("Time passed for " + operation_type + " operation: " + str(time_passed))
    elif operation_type == "re-write":
        sequential_read_write(file_path, file_size, record_length, "write")
        time_passed = sequential_read_write(file_path, file_size, record_length, "write")
        print("Time passed for " + operation_type + " operation: " + str(time_passed))

arr = ["write","read","re-read","re-write","random-read", "random-write"]    

for element in arr:
    #TEST1
    file_path = 'testfile.bin'
    file_size = 1024 * 1024 * 1  
    record_length = 8  
    operation = element
    create_test_file(file_path,file_size,record_length)
    select_scheduler(operation,file_size,record_length) 
    call_function(file_path,file_size,record_length,operation)

    #TEST2
    file_path = 'testfile.bin'
    file_size = 1024 * 1024 * 32  
    record_length = 8  
    operation = element
    create_test_file(file_path,file_size,record_length)
    select_scheduler(operation,file_size,record_length) 
    call_function(file_path,file_size,record_length,operation)

    #TEST3
    file_path = 'testfile.bin'
    file_size = 1024 * 1024 * 4 
    record_length = 64  
    operation = element
    create_test_file(file_path,file_size,record_length)
    select_scheduler(operation,file_size,record_length) 
    call_function(file_path,file_size,record_length,operation)


    #TEST4
    file_path = 'testfile.bin'
    file_size = 1024 * 1024 * 512 
    record_length = 64  
    operation = element
    create_test_file(file_path,file_size,record_length)
    select_scheduler(operation,file_size,record_length) 
    call_function(file_path,file_size,record_length,operation)


    #TEST5
    file_path = 'testfile.bin'
    file_size = 1024 * 1024 * 16 
    record_length = 512  
    operation = element
    create_test_file(file_path,file_size,record_length)
    select_scheduler(operation,file_size,record_length) 
    call_function(file_path,file_size,record_length,operation)


    #TEST6
    file_path = 'testfile.bin'
    file_size = 1024 * 1024 * 512 
    record_length = 512  
    operation = element
    create_test_file(file_path,file_size,record_length)
    select_scheduler(operation,file_size,record_length) 
    call_function(file_path,file_size,record_length,operation)



