ssh-keygen -t rsa -f ~/.ssh/id_rsa -N "" -q
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys