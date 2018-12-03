# Install git
sudo yum install git

# Clone project
git clone https://github.com/hks-epod/fto-scrape.git

# Install conda and make it system Python
# Make sure that end of anaconda installation you select yes when installer asks
# whether to prepend to system Python
wget http://repo.continuum.io/archive/Anaconda3-5.2.0-Linux-x86_64.sh
bash Anaconda3-5.2.0-Linux-x86_64.sh
source .bashrc

# Get Chrome driver
wget https://chromedriver.storage.googleapis.com/2.42/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv /usr/bin/chromedriver chromedriver

# Get Google Chrome
curl https://intoli.com/install-google-chrome.sh | bash
sudo mv /usr/bin/google-chrome-stable /usr/bin/google-chrome
google-chrome --version && which google-chrome

# Install pip
pip install --upgrade pip

# Install packages listed in requirements.txt
pip install -r /home/ec2-user/fto-scrape/backend/ec2/requirements.txt

# Make directory for log files
mkdir nrega_output

