if [ ! -d instance ]; then
    git clone https://github.com/userdashboard/dashboard.git instance
    cd instance
    npm install mongodb mocha puppeteer@2.1.1 --no-save
else 
    cd instance
fi
rm -rf node_modules/@userdashboard/storage-mongodb
mkdir -p node_modules/@userdashboard/storage-mongodb
cp ../index.js node_modules/@userdashboard/storage-mongodb
cp -R ../src node_modules/@userdashboard/storage-mongodb

NODE_ENV=testing \
FAST_START=true \
DASHBOARD_SERVER=http://localhost:9000 \
DOMAIN=localhost \
STORAGE_ENGINE=@userdashboard/storage-mongodb \
MONGODB_URL=mongodb://localhost:27017 \
MONGODB_DATABASE=dashboard \
GENERATE_SITEMAP_TXT=false \
GENERATE_API_TXT=false \
npm test