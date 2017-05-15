"""Make the custom certificate and private key files used by TLS tests.

Code heavily borrowed from Lib/tests/make_ssl_certs.py in CPython.
"""

import os
import shutil
import tempfile
from subprocess import *

req_template = """
    [req]
    distinguished_name     = req_distinguished_name
    x509_extensions        = req_x509_extensions
    prompt                 = no

    [req_distinguished_name]
    C                      = XY
    L                      = Dask-distributed
    O                      = Dask
    CN                     = {hostname}

    [req_x509_extensions]
    subjectAltName         = @san

    [san]
    DNS.1 = {hostname}

    [ca]
    default_ca      = CA_default

    [CA_default]
    dir = cadir
    database  = $dir/index.txt
    crlnumber = $dir/crl.txt
    default_md = sha1
    default_days = 3600
    default_crl_days = 3600
    certificate = tls-ca-cert.pem
    private_key = tls-ca-key.pem
    serial    = $dir/serial
    RANDFILE  = $dir/.rand

    policy          = policy_match

    [policy_match]
    countryName             = match
    stateOrProvinceName     = optional
    organizationName        = match
    organizationalUnitName  = optional
    commonName              = supplied
    emailAddress            = optional

    [policy_anything]
    countryName   = optional
    stateOrProvinceName = optional
    localityName    = optional
    organizationName  = optional
    organizationalUnitName  = optional
    commonName    = supplied
    emailAddress    = optional

    [v3_ca]
    subjectKeyIdentifier=hash
    authorityKeyIdentifier=keyid:always,issuer
    basicConstraints = CA:true
    """

here = os.path.abspath(os.path.dirname(__file__))

def make_cert_key(hostname, sign=False):
    print("creating cert for " + hostname)
    tempnames = []
    for i in range(3):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            tempnames.append(f.name)
    req_file, cert_file, key_file = tempnames
    try:
        req = req_template.format(hostname=hostname)
        with open(req_file, 'w') as f:
            f.write(req)
        args = ['req', '-new', '-days', '3650', '-nodes',
                '-newkey', 'rsa:1024', '-keyout', key_file,
                '-config', req_file]
        if sign:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                tempnames.append(f.name)
                reqfile = f.name
            args += ['-out', reqfile ]

        else:
            args += ['-x509', '-out', cert_file ]
        check_call(['openssl'] + args)

        if sign:
            args = ['ca', '-config', req_file, '-out', cert_file, '-outdir', 'cadir',
                    '-policy', 'policy_anything', '-batch', '-infiles', reqfile ]
            check_call(['openssl'] + args)


        with open(cert_file, 'r') as f:
            cert = f.read()
        with open(key_file, 'r') as f:
            key = f.read()
        return cert, key
    finally:
        for name in tempnames:
            os.remove(name)

TMP_CADIR = 'cadir'

def unmake_ca():
    shutil.rmtree(TMP_CADIR)

def make_ca():
    os.mkdir(TMP_CADIR)
    with open(os.path.join('cadir','index.txt'),'a+') as f:
        pass # empty file
    #with open(os.path.join('cadir','crl.txt'),'a+') as f:
        #f.write("00")
    with open(os.path.join('cadir','index.txt.attr'),'w+') as f:
        f.write('unique_subject = no')

    with tempfile.NamedTemporaryFile("w") as t:
        t.write(req_template.format(hostname='our-ca-server'))
        t.flush()
        with tempfile.NamedTemporaryFile() as f:
            args = ['req', '-new', '-days', '3650', '-extensions', 'v3_ca', '-nodes',
                    '-newkey', 'rsa:2048', '-keyout', 'tls-ca-key.pem',
                    '-out', f.name,
                    '-subj', '/C=XY/L=Dask-distributed/O=Dask CA/CN=our-ca-server']
            check_call(['openssl'] + args)
            args = ['ca', '-config', t.name, '-create_serial',
                    '-out', 'tls-ca-cert.pem', '-batch', '-outdir', TMP_CADIR,
                    '-keyfile', 'tls-ca-key.pem', '-days', '3650',
                    '-selfsign', '-extensions', 'v3_ca', '-infiles', f.name ]
            check_call(['openssl'] + args)
            #args = ['ca', '-config', t.name, '-gencrl', '-out', 'revocation.crl']
            #check_call(['openssl'] + args)


if __name__ == '__main__':
    os.chdir(here)
    cert, key = make_cert_key('localhost')
    with open('tls-self-signed-cert.pem', 'w') as f:
        f.write(cert)
    with open('tls-self-signed-key.pem', 'w') as f:
        f.write(key)

    # For certificate matching tests
    make_ca()
    with open('tls-ca-cert.pem', 'r') as f:
        ca_cert = f.read()

    cert, key = make_cert_key('localhost', sign=True)
    with open('tls-cert.pem', 'w') as f:
        f.write(cert)
    with open('tls-cert-chain.pem', 'w') as f:
        f.write(cert)
        f.write(ca_cert)
    with open('tls-key.pem', 'w') as f:
        f.write(key)
    with open('tls-key-cert.pem', 'w') as f:
        f.write(key)
        f.write(cert)

    unmake_ca()
