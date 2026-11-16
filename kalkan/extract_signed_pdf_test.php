<?php
/** Local diagnostic: extract embedded PDF ONLY after KalkanCrypt signature verification.
 * Run inside Docker with KalkanCrypt loaded and official GOST certificates available.
 * Do not treat extracted PDF as the original pre-signing PDF from PostgreSQL.
 */
declare(strict_types=1);
ini_set('display_errors', '0');
if (PHP_SAPI !== 'cli' || $argc !== 3) { fwrite(STDERR, "Usage: php extract_signed_pdf_test.php /work/test.p7m /work/signed_from_cms.pdf\n"); exit(2); }
[$script, $cmsPath, $outputPath] = $argv;
if (!is_file($cmsPath) || !is_readable($cmsPath)) { fwrite(STDERR, "CMS_FILE_MISSING\n"); exit(2); }
if (is_file($outputPath)) { fwrite(STDERR, "OUTPUT_EXISTS: choose a new output name\n"); exit(2); }
if (!function_exists('KalkanCrypt_VerifyData')) { fwrite(STDERR, "KALKAN_EXTENSION_MISSING\n"); exit(2); }
$cms = file_get_contents($cmsPath);
if ($cms === false || strlen($cms) < 50 || strlen($cms) > 8 * 1024 * 1024) { fwrite(STDERR, "CMS_SIZE_INVALID\n"); exit(2); }
$initialized = false;
try {
  $r = KalkanCrypt_Init();
  if ($r !== 0 && $r !== null) throw new RuntimeException('INIT_FAILED');
  $initialized = true;
  foreach (['root_gost_2022.cer'=>0x201, 'nca_gost_2022.cer'=>0x202] as $name=>$type) {
    $p = __DIR__.'/certs/'.$name;
    if (!is_file($p) || KalkanCrypt_X509LoadCertificateFromFile($type, $p) !== 0) throw new RuntimeException('CERT_LOAD_FAILED');
  }
  $content = ''; $info = ''; $cert = ''; $orig = '';
  $r = KalkanCrypt_VerifyData('', 0x812, $orig, 0, base64_encode($cms), $content, $info, $cert);
  if ($r !== 0 || !str_contains($info, 'CMS Verify - OK')) throw new RuntimeException('CMS_VERIFY_FAILED');
  // The supplied SDK has been seen returning raw embedded content; support exact Base64 PDF too.
  $pdf = $content;
  if (!str_starts_with($pdf, '%PDF-')) {
    $decoded = base64_decode($content, true);
    if ($decoded !== false && str_starts_with($decoded, '%PDF-')) $pdf = $decoded;
  }
  if (!str_starts_with($pdf, '%PDF-') || strlen($pdf) < 100) throw new RuntimeException('EMBEDDED_CONTENT_NOT_PDF');
  $bytes = file_put_contents($outputPath, $pdf, LOCK_EX);
  if ($bytes === false || $bytes !== strlen($pdf)) throw new RuntimeException('WRITE_FAILED');
  echo json_encode(['signature_check'=>'OK', 'output_file'=>$outputPath, 'pdf_bytes'=>strlen($pdf), 'pdf_sha256'=>hash('sha256',$pdf), 'note'=>'TEST ONLY: source PDF was extracted from this same CMS, not read from PostgreSQL'], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES),"\n";
} catch (Throwable $e) { fwrite(STDERR, $e->getMessage()."\n"); exit(1); }
finally { if ($initialized && function_exists('KalkanCrypt_Finalize')) KalkanCrypt_Finalize(); }
