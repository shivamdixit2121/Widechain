# Widechain
Running the simulator <br>
main.py [-h] [--N N] [--S S] [--BS BS] [--IA IA] [--duration DURATION] <br>

optional arguments: <br>
  -h, --help  :       show this help message and exit <br>
  --N         :       Node count (defalut = 64) <br>
  --S         :       Shard count (defalut = 4) <br>
  --BS        :       Block Size in KB (defalut = 32) <br>
  --IA        :       Interarrival time in seconds (defalut = 15) <br>
  --duration  :       Simulation duration in minutes (defalut = 15) <br>
  
  Example :  main.py --N 32 --S 8 --duration 30 <br>

A output file with name based on input arguments is created with the final measurments.
