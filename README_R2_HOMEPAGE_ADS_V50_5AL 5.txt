BoatSpotMedia v50.5AL - Homepage Ads in Cloudflare R2 + Spanish Home + Favicon

R2 ENV VARIABLES REQUIRED:
R2_ACCOUNT_ID
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
R2_BUCKET_NAME
R2_PUBLIC_URL

Also accepted:
CLOUDFLARE_ACCOUNT_ID
CLOUDFLARE_R2_ACCESS_KEY_ID
CLOUDFLARE_R2_SECRET_ACCESS_KEY
CLOUDFLARE_R2_BUCKET
CLOUDFLARE_R2_PUBLIC_URL

Homepage ad images upload to:
ads/homepage/<filename>

The database stores the public URL in:
homepage_ad_campaign.image_url

If R2 credentials are missing or fail during test, the app falls back to:
/static/uploads/homepage_ads/

Spanish:
Home page EN / ES language toggle. Session remembers selected language.
