#!/usr/bin/env nu

def load_secrets_and_start_binary [] {
    print "Loading SOPS secrets..."
    
    try {
        let secrets = sops -d secrets/secrets.json | from json
        
        mut env_vars = {}
        for secret in ($secrets | transpose key value) {
            $env_vars = ($env_vars | insert $secret.key $secret.value)
        }
        
        let secret_count = $secrets | transpose key value | length
        print $"Loaded ($secret_count) secrets, starting Rust binary..."

                
        with-env $env_vars {
            exec ./pv_api
        }
        
    } catch { |e|
        print $"Error: ($e)"
        exit 1
    }
}

load_secrets_and_start_binary
