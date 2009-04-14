require 'rubygems'
require 'twitter_search'
require 'sequel'
require 'memcache'
require 'cgi'
 
module TwitterSearchClient

  class << self
    attr_accessor :master_db_path, :agent_name, :memcached_server
    def init
      yield if block_given?
      @db ||= Sequel.connect(@master_db_path)
      @search_client = TwitterSearch::Client.new @agent_name
      @cache ||= MemCache.new(@memcached_server,
                              :c_threshold => 10_000,
                              :compression => true,
                              :debug => false,
                              :namespace => 'query_tweets',
                              :readonly => false,
                              :urlencode => false)
    end
    
    def fetch_all(client_id=0, num_of_clients=1)
      num_of_clients=1 unless num_of_clients
      client_id=0 if client_id.nil? || client_id < 0
      @db[:queries].all.each{|query|
        fetch(query) if query[:id]%num_of_clients==client_id
      }
    end
    
    def fetch(query)
      temp_latest_id = latest_id = query[:latest_id] || 0
      ptweets = @db[:tweets]
      pquery_tweets = @db[:query_tweets]
      1.upto(15) do |page|
        puts "---- entering page #{page} of #{query[:q]} query.id: #{query[:id]}  ---- "
        begin 
          tweets = @search_client.query(:q=> query[:q], :since_id => latest_id, :rpp => '100', :page => page) 
        rescue
          puts $!
          nil
        rescue Timeout::Error
          puts "Timeout!!!" + $!
          nil
        end
        if tweets
          tweets.each do |t|
            temp_latest_id = t.id if temp_latest_id < t.id
            begin
              ptweets << {
                :to_user_id => t.to_user_id,
                :from_user_id => t.from_user_id,
                :language => t.language, 
                :to_user => (t.to_user || nil), 
                :iso_language_code => t.iso_language_code,
                :from_user => (t.from_user || nil), 
                :profile_image_url => t.profile_image_url,
                :id => t.id, 
                :text => t.text, 
                :tweeted_at => Time.parse(t.created_at)
               }
            rescue 
              puts $!
            end
            begin
              pquery_tweets << {
                :tweet_id => t.id,
                :query_id => query[:id]
               }
            rescue 
              puts $!
            end
          end
        end
        break if !tweets || tweets.size < 100
      end
      @db[:queries].filter(:id=>query[:id]).update(:latest_id => temp_latest_id, :last_queried => Time.now.utc)
      @cache[query[:id]] = @db[:tweets].join(:query_tweets, :tweet_id => :tweets__id).filter(:query_tweets__query_id=>query[:id]).order(:query_tweets__tweet_id.desc). \
        select(:tweets__id, :tweets__to_user_id, :tweets__from_user_id, :tweets__language, :tweets__to_user, :tweets__iso_language_code, :tweets__from_user, :tweets__profile_image_url, :tweets__text, :tweets__tweeted_at, :tweets__created_at).limit(100).all
    end
  end
end