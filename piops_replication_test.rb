require 'rubygems'
require 'jdbc/mysql'
require 'java'
require 'concurrent/atomic/atomic_fixnum'

# CREATE TABLE `test` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `text` text NOT NULL,
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

Java::com.mysql.jdbc.Driver

CHUNK_SIZE = 1000 # Try 1100 or 1200 - lag trajectory on PIOPS vs GP2 might split negative/positive
NUM_THREADS = 80
THROUGHPUT_TARGET = 800000

# How many bytes are we sending this second?
$counter = 0

COUNTER_MONITOR = java.lang.Object.new
COUNTER_MONITOR.synchronized do $counter = 0 end

class LoopThread < java.lang.Thread
  ##
  # Implements the run loop, checking @active and @limit each iteration and trapping exceptions in @exception
  def run
    userurl = "jdbc:mysql://dw-load-test-gp2.c1alamqldza3.us-east-1.rds.amazonaws.com/dw_load_test"
    connection = java.sql.DriverManager.get_connection(userurl, "admin", "thitApbers2")
    statement = connection.create_statement

    while true do
      payload = (0...CHUNK_SIZE).map { ('a'..'z').to_a[rand(26)] }.join
      query = %Q{INSERT INTO dw_load_test.test (text) values ('#{ payload }')}

      # Execute the query
      affected_rows = statement.execute_update(query)

      raise "Problem inserting" unless affected_rows == 1

      COUNTER_MONITOR.synchronized do
        $counter = $counter + CHUNK_SIZE

        if $counter > ( THROUGHPUT_TARGET - (CHUNK_SIZE * NUM_THREADS) ) # Simplistically, we would just stop when over the target. We're leading a moving target a bit here, attempt to stop the fleet of threads just in advance of it.
          COUNTER_MONITOR.wait
        end
      end
    end
  end
end

class TimerThread < java.lang.Thread
  def run
    while true
      sleep 1
      COUNTER_MONITOR.synchronized do
        puts "Tick. Sent: #{$counter}"

        $counter = 0
        COUNTER_MONITOR.notifyAll
      end
    end
  end
end

NUM_THREADS.times { LoopThread.new.start }
TimerThread.new.start
