export MODULE=$1

# Create directory for logs, if necessary.
mkdir -p /mnt/share/logs/$MODULE

# Redirect all subsequent logs.
# The $BATCH_TASK_INDEX variable is set by Google Batch.
exec &> /mnt/share/logs/$MODULE/log.$BATCH_TASK_INDEX.log

echo ">>> Update APT"
sudo apt -y update

echo ">>> Install packages"
sudo apt -y install python3 python3-pip python3-setuptools

echo ">>> Update PIP and setuptools"
sudo python3 -m pip install --upgrade pip setuptools
echo $?

echo ">>> Install packages"
sudo python3 -m pip install -r /mnt/share/code/requirements.tt
echo $?

echo ">>> Run script"
sudo python3 /mnt/share/code/${MODULE}.py ${BATCH_TASK_INDEX}

echo ">>> Completed"
echo $?
