#! /bin/sh

if [ "$#" != "2" ]; then
  echo "Usage : $0 path/to/jar-with-dependencies.jar jdbc-connector.sh"
  exit 1
fi

cat > $2 <<- EOF
#! /bin/sh

BIN=\`mktemp /tmp/algolia.XXXX\`
uudecode -o \$BIN \$0

command -v java >/dev/null 2>&1 || { echo >&2 "Java is required, please install it first."; exit 1; }

java -jar \$BIN
RETURN_VALUE=`echo $?`
rm -f \$BIN
exit \$RETURN_VALUE
EOF

uuencode $1 $1 >> $2
chmod +x $2
