# Bixi Datapipeline

Have a venv in .venv with the requirements.txt inside of it

## Systemd bixi service 
Edit bixi.service where it says EDIT_HERE

Put bixi.service here : 
/etc/systemd/system/bixi.service

sudo systemctl daemon-reload
sudo systemctl enable bixi.service
sudo systemctl start bixi.service


## Crontab to update with github at 3am

On update_if_needed.sh change the working dir
then add to crontab : 

crontab -e

0 3 * * * /home/yourname/projects/bixi/update_if_needed.sh >> /home/yourname/projects/bixi/update.log 2>&1
