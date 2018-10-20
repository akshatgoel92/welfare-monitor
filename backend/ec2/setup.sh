# Install conda and make it system Python 
wget http://repo.continuum.io/archive/Anaconda3-5.2.0-Linux-x86_64.sh
bash Anaconda3-5.2.0-Linux-x86_64.sh
source .bashrc

# Get Chrome driver
wget https://chromedriver.storage.googleapis.com/2.42/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/bin/chromedriver

# Get Google Chrome
cd /usr/bin/
curl https://intoli.com/install-google-chr
sudo mv /usr/bin/google-chrome-stable /usr/bin/google-chrome
google-chrome --version && which google-chrome

# Install git
sudo yum install git

# Clone project
git clone https://github.com/hks-epod/gma-scrape.git

# Install pip
pip install --upgrade pip

# Install packages listed in requirements.txt
pip install -r /home/ec2-user/gma-scrape/backend/ec2/requirements.txt

# Make directory for log files
mkdir nrega_output


