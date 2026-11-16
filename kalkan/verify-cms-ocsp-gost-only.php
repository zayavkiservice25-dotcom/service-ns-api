<?php
/** Local diagnostic, GOST certificates only; no writes or automatic approval. */
declare(strict_types=1);
if (PHP_SAPI !== 'cli') { exit(2); }
function stop(string $message, int $code=1): never { fwrite(STDERR, $message . PHP_EOL); exit($code); }
if ($argc !== 2) stop('Usage: php verify-cms-ocsp-gost-only.php /work/test.p7m',2);
$path = $argv[1];
if (!is_file($path) || !is_readable($path)) stop('CMS not readable',2);
$length = filesize($path);
if ($length === false || $length < 20 || $length > 20*1024*1024) stop('CMS size invalid',2);
foreach (['KalkanCrypt_Init','KalkanCrypt_Finalize','KalkanCrypt_X509LoadCertificateFromFile','KalkanCrypt_VerifyData','KalkanCrypt_getCertFromCMS','KalkanCrypt_X509ValidateCertificate'] as $fn) {
    if (!function_exists($fn)) stop('Missing KalkanCrypt function: '.$fn,2);
}
$raw = file_get_contents($path);
if ($raw === false) stop('Cannot read CMS',2);
$cmsB64 = base64_encode($raw);
$flags = 0x2 | 0x10 | 0x800; // CMS, input base64, output base64
$started = false;
try {
    $init = KalkanCrypt_Init();
    if ($init !== 0 && $init !== null) stop('Init result: '.var_export($init,true));
    $started = true;
    foreach (['root_gost_2022.cer'=>0x201,'nca_gost_2022.cer'=>0x202] as $filename=>$type) {
        $certFile = __DIR__.'/certs/'.$filename;
        if (!is_file($certFile)) stop('Missing '.$certFile,2);
        $result = KalkanCrypt_X509LoadCertificateFromFile($type, $certFile);
        echo 'Load '.$filename.' => '.var_export($result,true).PHP_EOL;
        if ($result !== 0) stop('Could not load CA certificate');
    }
    $original = ''; $data = ''; $verificationInfo = ''; $certOut = '';
    $result = KalkanCrypt_VerifyData('', $flags, $original, 0, $cmsB64, $data, $verificationInfo, $certOut);
    echo 'CMS return => '.var_export($result,true).PHP_EOL;
    echo 'CMS Verify OK => '.(str_contains($verificationInfo, 'CMS Verify - OK') ? 'yes':'no').PHP_EOL;
    if ($result !== 0 || !str_contains($verificationInfo,'CMS Verify - OK')) stop('CMS signature failed');
    $signerCert = '';
    $result = KalkanCrypt_getCertFromCMS($cmsB64,1,$flags,$signerCert);
    echo 'getCertFromCMS return => '.var_export($result,true).PHP_EOL;
    if ($result !== 0 || $signerCert === '') stop('Signer certificate not returned');
    $validationInfo = ''; $ocspResponse = '';
    $url = 'http://ocsp.pki.gov.kz/';
    $result = KalkanCrypt_X509ValidateCertificate($signerCert,0x404,$url,0,$validationInfo,0,$ocspResponse);
    echo 'OCSP result => '.var_export($result,true).PHP_EOL;
    echo 'OCSP response length => '.strlen($ocspResponse).PHP_EOL;
    echo 'OCSP details => '.substr($validationInfo,0,1800).PHP_EOL;
    if ($result !== 0) {
        echo 'SDK error => '.(function_exists('KalkanCrypt_GetLastErrorString') ? substr((string)KalkanCrypt_GetLastErrorString(),0,1800):'unavailable').PHP_EOL;
        stop('OCSP still failed; certificate status NOT confirmed');
    }
    echo 'OCSP SDK call successful. Additional chain, signing-time and authority checks still required.'.PHP_EOL;
} catch(Throwable $e) {
    stop('PHP or SDK exception: '.$e->getMessage());
} finally {
    if ($started) KalkanCrypt_Finalize();
}
