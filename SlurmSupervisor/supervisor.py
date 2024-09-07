import os
import sys
import asyncio
import argparse
import batch_runner
from utils import provision_instance

PROGRAM = sys.argv[1]
INSTANCES = []
NUM_REQUESTS = 0

async def instance_supervisor(chdir : str,
                            instance : str,
                            num_instances : int,
                            time : int,
                            check_freq : int):
    while True:
        if len(INSTANCES) < num_instances and NUM_REQUESTS < num_instances:
            #Need to submit a batch job for script
            provision_instance(chdir,
                               instance,
                               time)
            NUM_REQUESTS += 1
        else:
            
            await asyncio.sleep()

            


    return

async def execution_supervisor(inputs):
    while True:
        if len(inputs) == 0:
            return
        if check_idle_instances(INSTANCES):
            instance = get_idle_instance(INSTANCES)
            input = get_idle_inputs(inputs)
            instance.run(program, input)
            instance.status = 'RUNNING'

        if check_busy_instances(INSTANCES):
            for inst in get_busy_instances(INSTANCES):
                if check_completion(inst):
                    instance.status = 'IDLE'
                if check_provisioned(inst) is False:
                    inst.inputs.status = 'IDLE'
                    INSTANCES.remove(instance)
                    NUM_REQUESTS -= 1
    return





async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance",
                         type=str, 
                         default='cpu',
                         help='The type of instances to be provisioned (Default set to cpu)')
    parser.add_argument("--num_instances",
                        type=int,
                        default=8,
                        help="The max numbers of instances to provision (Default is 8)")
    parser.add_argument("--inputs",
                        type=str,
                        required=True,
                        help="Path to file containing the different inputs in CSV format on which to execute the program")
    parser.add_argument("--dir",
                        type=str,
                        default = os.getcwd(),
                        help="The directory in which to initialise the instances (Default is current directory)")
    parser.add_arugment("--time",
                        type=int,
                        default = 48,
                        help="The number of hours to provision any given instance (Default is 48 hours)")
    parser.add_argument("--check_freq",
                        type=int,
                        default = 10,
                        help="The number of minutes after which we check for ")

    args = parser.parse_args()

    #Parse the inputs
    inputs = parse_inputs(args.inputs)

    while True:
        #Finish running if all inputs have been cleared
        if len(inputs) == 0:
            exit()
        #Run these asynchronously
        instance_supervisor(args.instance, args.num_instances)
        execution_supervisor(inputs)

                    
if __name__ == "__main__":
    asyncio.run(main)