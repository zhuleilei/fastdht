#ifndef FASTDHT_CLIENT_H
#define FASTDHT_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

PHP_MINIT_FUNCTION(fastdht_client);
PHP_RINIT_FUNCTION(fastdht_client);
PHP_MSHUTDOWN_FUNCTION(fastdht_client);
PHP_RSHUTDOWN_FUNCTION(fastdht_client);
PHP_MINFO_FUNCTION(fastdht_client);

ZEND_FUNCTION(fastdht_client_set_const);

#ifdef __cplusplus
}
#endif

#endif	/* FASTDHT_CLIENT_H */
