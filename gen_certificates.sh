rm -f *.ks

keytool -genkey -alias users -keyalg RSA -validity 365 -keystore ./users.ks -storetype pkcs12 << EOF
123users
123users
Users.Users
TP2
SD2021
LX
LX
PT
yes
123users
123users
EOF

echo
echo
echo "Exporting Certificates"
echo
echo

keytool -exportcert -alias users -keystore users.ks -file users.cert << EOF
123users
EOF

echo "Creating Client Truststore"
cp cacerts client-ts.ks
keytool -importcert -file users.cert -alias users -keystore client-ts.ks << EOF
changeit
yes
EOF

keytool -genkey -alias files -keyalg RSA -validity 365 -keystore ./files.ks -storetype pkcs12 << EOF
123files
123files
FILES.FILES
TP2
SD2021
LX
LX
PT
yes
123files
123files
EOF

keytool -exportcert -alias files -keystore files.ks -file files.cert << EOF
123files
EOF

keytool -importcert -file files.cert -alias files -keystore client-ts.ks << EOF
changeit
yes
EOF

keytool -genkey -alias dir -keyalg RSA -validity 365 -keystore ./dir.ks -storetype pkcs12 << EOF
123dir
123dir
DIR.DIR
TP2
SD2021
LX
LX
PT
yes
123dir
123dir
EOF

keytool -exportcert -alias dir -keystore dir.ks -file dir.cert << EOF
123dir
EOF

keytool -importcert -file dir.cert -alias dir -keystore client-ts.ks << EOF
changeit
yes
EOF
