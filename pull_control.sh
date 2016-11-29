git -C ~/control/ stash save --keep-index
git -C ~/control/ stash drop
git -C ~/control/ fetch origin
git -C ~/control/ merge origin/master
sed -i -e 's/workermsisdn/workermsisdn_test/g' Control.java
sed -i -e 's/workermsisdn/worknumber_test/g' Control.java
sed -i -e 's/workermsisdn/workorder_test/g' Control.java