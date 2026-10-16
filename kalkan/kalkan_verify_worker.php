<?php
/** CLI-only CMS verification; no private keys, no DB; stdout is one JSON object. */
declare(strict_types=1);
ini_set('display_errors', '0');
if (PHP_SAPI !== 'cli') exit(2);
function result(bool $ok, string $reason, array $more=[]): never {
    echo json_encode(['ok'=>$ok,'reason'=>$reason]+$more, JSON_UNESCAPED_UNICODE|JSON_INVALID_UTF8_SUBSTITUTE), "\n";
    exit($ok?0:1);
}
if ($argc!==3) result(false,'USAGE');
[$script,$cmsPath,$pdfPath]=$argv;
if (!is_file($cmsPath)||!is_file($pdfPath)) result(false,'MISSING_INPUT');
$cms=file_get_contents($cmsPath); $pdf=file_get_contents($pdfPath);
if ($cms===false||$pdf===false||strlen($cms)>8*1024*1024||strlen($cms)<50||strlen($pdf)>8*1024*1024||strlen($pdf)<50) result(false,'INVALID_INPUT');
foreach (['KalkanCrypt_Init','KalkanCrypt_X509LoadCertificateFromFile','KalkanCrypt_VerifyData','KalkanCrypt_getCertFromCMS','KalkanCrypt_X509ValidateCertificate'] as $fn) if (!function_exists($fn)) result(false,'MISSING_EXTENSION');
$init=false;
try {
  $r=KalkanCrypt_Init(); if ($r!==0 && $r!==null) result(false,'INIT_FAILED'); $init=true;
  foreach (['root_gost_2022.cer'=>0x201,'nca_gost_2022.cer'=>0x202] as $file=>$type) {
    $p=__DIR__.'/certs/'.$file;
    if (!is_file($p)||KalkanCrypt_X509LoadCertificateFromFile($type,$p)!==0) result(false,'CA_LOAD_FAILED');
  }
  $flags=0x2|0x10|0x800;
  $content=''; $info=''; $cert=''; $orig=''; $cmsB64=base64_encode($cms);
  $r=KalkanCrypt_VerifyData('',$flags,$orig,0,$cmsB64,$content,$info,$cert);
  if ($r!==0||!str_contains($info,'CMS Verify - OK')) result(false,'CMS_INVALID',['cms_code'=>$r]);
  // No conversion, normalization, or PDF regeneration allowed. Some SDK builds
  // return encapsulated content as Base64: accept only strict, exact decoded match.
  $same=hash_equals(hash('sha256',$pdf),hash('sha256',$content));
  if (!$same) {
    $decoded=base64_decode($content,true);
    $same=$decoded!==false && hash_equals(hash('sha256',$pdf),hash('sha256',$decoded));
  }
  if (!$same) result(false,'SIGNED_PDF_DIFFERENT',[
    'expected_pdf_sha256'=>hash('sha256',$pdf),
    'attached_content_sha256'=>hash('sha256',$content),
    'attached_length'=>strlen($content),
    'expected_length'=>strlen($pdf)
  ]);
  $signerCert=''; $r=KalkanCrypt_getCertFromCMS($cmsB64,1,$flags,$signerCert);
  if ($r!==0||$signerCert==='') result(false,'SIGNER_CERT_MISSING',['cert_code'=>$r]);
  // Certificate is returned by the SDK for CMS signer #1. Accept DER or its exact
  // PEM/Base64 encoding only; never hash arbitrary text or log certificate data.
  $certText=trim($signerCert);
  if (str_starts_with($certText,'-----BEGIN CERTIFICATE-----')) {
    $certText=preg_replace('~-----BEGIN CERTIFICATE-----|-----END CERTIFICATE-----|\s+~','',$certText);
  }
  $certDer=base64_decode($certText,true);
  if ($certDer===false && strlen($signerCert)>100 && ord($signerCert[0])===0x30) $certDer=$signerCert;
  if (!is_string($certDer) || strlen($certDer)<100 || ord($certDer[0])!==0x30)
    result(false,'SIGNER_CERT_FORMAT_UNKNOWN');
  $signerFingerprint=hash('sha256',$certDer);
  $outInfo=''; $ocsp='';
  $r=KalkanCrypt_X509ValidateCertificate($signerCert,0x404,'http://ocsp.pki.gov.kz/',0,$outInfo,0,$ocsp);
  if ($r!==0||!str_contains($outInfo,'Verify chain and certificates: - OK.'))
    result(false,'OCSP_OR_CHAIN_FAILED',['ocsp_code'=>$r]);
  // Successful verification is NOT equivalent to completed identity/authority or
  // signing-time revocation validation. Never auto-approve the order here.
  result(true,'CRYPTO_AND_CURRENT_OCSP_OK_PENDING_AUTHORITY',[
    'pdf_sha256'=>hash('sha256',$pdf), 'cms_sha256'=>hash('sha256',$cms),
    'signer_cert_sha256'=>$signerFingerprint,
    'ocsp_response_preserved'=>strlen($ocsp)>0
  ]);
} catch (Throwable $e) { result(false,'SDK_EXCEPTION'); }
finally { if ($init && function_exists('KalkanCrypt_Finalize')) KalkanCrypt_Finalize(); }
