tempfile="awsdeploytemp.zip"
ownname=$(basename "$0")

pip3 install bs4 requests --target .

rm -f $tempfile

zip -rq $tempfile ./* -x $ownname -x *.git* && aws lambda update-function-code --function-name rescue-dog-tracker --zip-file fileb://./$tempfile > /dev/null

RESULT=$?
if [ $RESULT -eq 0 ]; then
  echo "Successfully deployed code"
else
  echo "Failed"
fi

rm $tempfile
