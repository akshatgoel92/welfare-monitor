# Export PYTHONPATH=. 
export PYTHONPATH=.

# Get the script from the database
python script/get_script.py 30 0 0 1 1

# Put the script in the database
python script/put_script.py 

