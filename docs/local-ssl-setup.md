# Install ssl cert for local https testing

# Setup 

Install [mkcerts][2] and run `mkcert -install` and after it the following command:
`mkcert -cert-file cert.pem -key-file key.pem 0.0.0.0 localhost 127.0.0.1 ::1 metax.csc.local 20.20.20.20`
Move the `cert.pem` and `key.pem` to `src/.certs` folder (create the folder if not present).

## Run SSL enabled development server

`python manage.py runsslserver --certificate .certs/cert.pem --key .certs/key.pem 8008`

[2]: https://github.com/FiloSottile/mkcert