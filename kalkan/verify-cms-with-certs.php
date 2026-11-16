<?php
// KalkanCrypt CMS diagnostic. CLI only. Does not update any database or trust status.
// Run: php -d extension=/opt/kalkan/kalkancrypt.so verify-cms.php /work/test.p7m
if (PHP_SAPI !== 'cli') { http_response_code(403); exit; }
function stop(string $message, int $code = 1): void { fwrite(STDERR, $message . PHP_EOL); exit($code); }
if ($argc !== 2) stop('Usage: verify-cms.php /path/to/file.p7m', 2);
if (!function_exists('KalkanCrypt_Init') || !function_exists('KalkanCrypt_VerifyData')) stop('KalkanCrypt extension is not loaded');
$file = $argv[1];
if (!is_file($file) || !is_readable($file)) stop('Cannot read CMS file');
$bytes = file_get_contents($file);
if ($bytes === false || strlen($bytes) < 20 || strlen($bytes) > 20 * 1024 * 1024) stop('Invalid CMS file size');
// SDK flags from kalkanFlags&constants.php:
// KC_SIGN_CMS=0x2; KC_IN_BASE64=0x10; KC_OUT_BASE64=0x800
$flags = 0x2 | 0x10 | 0x800;
$cmsBase64 = base64_encode($bytes);
$alias = '';
$originalData = '';  // attached CMS must carry content internally
$outData = '';
$outVerifyInfo = '';
$outCert = '';
try {
    $init = KalkanCrypt_Init();
    // Load public trust anchors and intermediate CA certs before verifying the CMS.
    $certDir = __DIR__ . '/certs';
    $certs = [
        'root_gost_2022.cer' => 0x201,  // KC_CERT_CA
        'nca_gost_2022.cer'  => 0x202,  // KC_CERT_INTERMEDIATE
        'root_rsa_2020.cer'  => 0x201,
        'nca_rsa_2022.cer'   => 0x202,
    ];
    foreach ($certs as $filename => $kind) {
        $certPath = $certDir . '/' . $filename;
        if (!is_file($certPath)) stop('Certificate not found: ' . $certPath);
        $loadResult = KalkanCrypt_X509LoadCertificateFromFile($kind, $certPath);
        echo 'CA load ' . $filename . ': ' . var_export($loadResult, true) . PHP_EOL;
        if ($loadResult !== 0) {
            stop('Failed to load CA: ' . $filename . '; ' . (function_exists('KalkanCrypt_GetLastErrorString') ? KalkanCrypt_GetLastErrorString() : ''));
        }
    }
    // The SDK examples demonstrate these argument positions.
    $result = KalkanCrypt_VerifyData($alias, $flags, $originalData, 0, $cmsBase64, $outData, $outVerifyInfo, $outCert);
    echo "KalkanCrypt_VerifyData return: " . var_export($result, true) . PHP_EOL;
    echo "VerifyInfo:\n" . substr((string)$outVerifyInfo, 0, 8000) . PHP_EOL;
    echo "Certificate returned: " . (strlen((string)$outCert) > 0 ? 'yes' : 'no') . PHP_EOL;
    echo "Signed content returned (length): " . strlen((string)$outData) . PHP_EOL;
    if (is_int($result) && $result === 0) {
        echo "SDK cryptographic verification returned success. Certificate chain, OCSP, signer authority, and PDF equality are NOT independently verified by this test.\n";
        exit(0);
    }
    echo "SDK error details: " . (function_exists('KalkanCrypt_GetLastErrorString') ? substr((string)KalkanCrypt_GetLastErrorString(), 0, 2000) : 'unavailable') . PHP_EOL;
    exit(1);
} catch (Throwable $e) {
    stop('SDK exception: ' . $e->getMessage());
} finally {
    if (function_exists('KalkanCrypt_Finalize')) KalkanCrypt_Finalize();
}
