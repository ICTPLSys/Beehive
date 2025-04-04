#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include <chrono>
#include <cstring>
#include <iostream>

void handleErrors() {
    // ERR_print_errors_fp(stderr);
    abort();
}

RSA* generateKeyPair() {
    RSA* rsa = RSA_new();
    BIGNUM* e = BN_new();
    if (!BN_set_word(e, RSA_F4)) {
        handleErrors();
    }

    if (!RSA_generate_key_ex(rsa, 2048, e, NULL)) {
        handleErrors();
    }

    BN_free(e);
    return rsa;
}

RSA* RSA_init() { return generateKeyPair(); }

void RSA_encrypt(const char* rawText, RSA* rsa) {
    int plaintext_len = strlen(rawText);
    int rsa_size = RSA_size(rsa);
    unsigned char* ciphertext = new unsigned char[rsa_size];
    unsigned char* plaintext_chunk = new unsigned char[rsa_size - 42];

    int total_ciphertext_len = 0;
    int offset = 0;
    while (offset < plaintext_len) {
        int chunk_len = std::min(rsa_size - 42, plaintext_len - offset);
        std::memcpy(plaintext_chunk, rawText + offset, chunk_len);

        int len = RSA_public_encrypt(chunk_len, plaintext_chunk, ciphertext,
                                     rsa, RSA_PKCS1_OAEP_PADDING);
        if (len == -1) {
            handleErrors();
        }

        total_ciphertext_len += len;
        offset += chunk_len;
    }

    delete[] ciphertext;
    delete[] plaintext_chunk;
}
