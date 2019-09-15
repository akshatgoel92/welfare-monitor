# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Make sure that the Python path contains the current folder
export PYTHONPATH=.

# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Update the camp table
python ./script/put_camp.py

# Get the static script
python ./script/get_static_script.py --pilot=0

# Get the dynamic script
python ./script/get_dynamic_script.py --pilot=0 --window_length=7

# Put production scripts in the scripts table  in the database
python ./script/put_script.py 

