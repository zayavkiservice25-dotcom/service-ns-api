<?php
// CLI-only diagnostics; never exposed as a public endpoint.
header_remove();
echo json_encode([
 'php' => PHP_VERSION,
 'zts' => PHP_ZTS,
 'extension' => extension_loaded('kalkancrypt'),
 'init' => function_exists('KalkanCrypt_Init'),
 'verify' => function_exists('KalkanCrypt_VerifyData'),
 'validate_cert' => function_exists('KalkanCrypt_X509ValidateCertificate'),
], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE), "\n";
