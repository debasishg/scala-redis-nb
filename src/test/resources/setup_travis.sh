!/bin/sh

# Install Redis
wget http://download.redis.io/releases/redis-2.8.9.tar.gz
tar -xzf redis-2.8.9.tar.gz
cd redis-2.8.9
make
sudo make install

# Start Redis Server
/usr/local/bin/redis-server &
