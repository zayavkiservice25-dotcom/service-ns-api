<?php
/**
 * LOCAL diagnostic only: CMS signature + signer certificate + live OCSP.
 * No database, no private keys, no document upload to third parties other than
 * standard OCSP requests to the configured NCA responder.
 * Result must NOT be used as automatic approval of a legal document.
 * Usage: php -d extension=/opt/kalkan/kalkancrypt.so /work/verify-cms-ocsp.php /work/test.p7m
 */
declare(strict_types=1);
if (PHP_SAPI !== 'cli') { http_response_code(403); exit(2); }
function fail(string $why, int $code = 1): never { fwrite(STDERR, "ERROR: $why\n"); exit($code); }
if ($argc !== 2) fail('Usage: verify-cms-ocsp.php /path/to/test.p7m', 2);
foreach (['KalkanCrypt_Init', 'KalkanCrypt_VerifyData', 'KalkanCrypt_getCertFromCMS', 'KalkanCrypt_X509ValidateCertificate', 'KalkanCrypt_X509LoadCertificateFromFile'] as $f) {
    if (!function_exists($f)) fail('Missing SDK function ' . $f, 2);
}
$filename = $argv[1];
if (!is_file($filename) || !is_readable($filename)) fail('CMS file not found/readable', 2);
$raw = file_get_contents($filename);
if ($raw === false || strlen($raw) < 20 || strlen($raw) > 20 * 1024 * 1024) fail('Invalid CMS size', 2);
// Flags from the supplied official SDK samples.
$KC_CERT_CA=0x201; $KC_CERT_INTERMEDIATE=0x202; $KC_SIGN_CMS=0x2;
$KC_IN_BASE64=0x10; $KC_OUT_BASE64=0x800; $KC_USE_OCSP=0x404;
$cmsFlags=$KC_SIGN_CMS|$KC_IN_BASE64|$KC_OUT_BASE64;
$cmsB64=base64_encode($raw);
$ocspUrl=getenv('NCA_OCSP_URL') ?: 'http://ocsp.pki.gov.kz/';
if (!preg_match('~^https?://ocsp\.pki\.gov\.kz(?::\d+)?/~i', $ocspUrl)) fail('Unexpected OCSP URL',2);
$initialized=false;
try {
    $init=KalkanCrypt_Init();
    if ($init !== 0 && $init !== null) fail('KalkanCrypt_Init error: ' . var_export($init,true));
    $initialized=true;
    foreach (['root_gost_2022.cer'=>$KC_CERT_CA,'nca_gost_2022.cer'=>$KC_CERT_INTERMEDIATE,'root_rsa_2020.cer'=>$KC_CERT_CA,'nca_rsa_2022.cer'=>$KC_CERT_INTERMEDIATE] as $name=>$kind) {
        $p=__DIR__.'/certs/'.$name;
        if (!is_file($p)) fail('Missing trust certificate '.$name);
        $r=KalkanCrypt_X509LoadCertificateFromFile($kind,$p);
        echo "CA $name: ",var_export($r,true),"\n";
        if ($r !== 0) fail('Unable to load CA '.$name);
    }
    $outData='';$verifyInfo='';$verifyCert='';$alias='';$original='';
    $verified=KalkanCrypt_VerifyData($alias,$cmsFlags,$original,0,$cmsB64,$outData,$verifyInfo,$verifyCert);
    echo 'CMS verification result: ',var_export($verified,true),'\n';
    echo "CMS info:\n",substr((string)$verifyInfo,0,1800),"\n";
    if ($verified !== 0 || !preg_match('/CMS Verify\s*-\s*OK/i',(string)$verifyInfo)) fail('CMS cryptographic verification not successful');
    echo 'Attached content bytes: ',strlen((string)$outData),"\n";
    // SDK sample uses signer index 1. Obtain certificate separately; VerifyData often returns no cert.
    $signerCert='';
    $r=KalkanCrypt_getCertFromCMS($cmsB64,1,$cmsFlags,$signerCert);
    echo 'getCertFromCMS result: ',var_export($r,true),"\n";
    echo 'Signer certificate returned: ',(is_string($signerCert)&&$signerCert!=='' ? 'yes' : 'no'),"\n";
    if ($r !== 0 || !is_string($signerCert) || $signerCert==='') {
        echo 'SDK error: ',function_exists('KalkanCrypt_GetLastErrorString') ? substr((string)KalkanCrypt_GetLastErrorString(),0,500) : 'n/a',"\n";
        fail('Signer certificate extraction failed');
    }
    $outInfo='';$ocspResponse='';
    // Do NOT bypass certificate validity time checks (flags=0).
    $r=KalkanCrypt_X509ValidateCertificate($signerCert,$KC_USE_OCSP,$ocspUrl,0,$outInfo,0,$ocspResponse);
    echo 'OCSP validation result: ',var_export($r,true),"\n";
    echo 'OCSP details: ',substr((string)$outInfo,0,1500),"\n";
    echo 'OCSP response received: ',strlen((string)$ocspResponse)>0?'yes':'no',"\n";
    if ($r !== 0) {
        echo 'SDK error: ',function_exists('KalkanCrypt_GetLastErrorString')?substr((string)KalkanCrypt_GetLastErrorString(),0,600):'n/a',"\n";
        fail('Certificate OCSP validation not successful');
    }
    echo "LOCAL CHECKS PASSED: CMS cryptography and SDK OCSP call. Still need original-PDF equality, signer authority, archival controls and checks at signing time.\n";
} catch (Throwable $e) {
    fail('Exception from SDK: '.$e->getMessage());
} finally {
    if ($initialized && function_exists('KalkanCrypt_Finalize')) KalkanCrypt_Finalize();
}
