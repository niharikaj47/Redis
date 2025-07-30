#include <iostream>
#include <string>
#include <cstring>
#include <arpa/inet.h>
#include <thread>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <algorithm>
#include <unordered_map>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <stdint.h>
#include <netdb.h>

// Define the structure for an entry in the key-value store
struct Entry {
    std::string value;
    std::chrono::steady_clock::time_point expireAt; // optional expiry
};

std::unordered_map<std::string, Entry> store;
std::mutex store_mutex;
int port = 6379;
std::string isReplica = "";
std::string replicaHost;
int replicaPort = 0;
int replicaFd = -1;

std::string config_dir = "/tmp";             // Default dir
std::string config_dbfilename = "dump.rdb";  // Default filename

std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
int master_repl_offset = 0;

void connectToMasterAndSendPing(const std::string& host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("Socket creation failed");
        return;
    }
    // Set up the server address structure
    // Note: This is a blocking connect, which is fine for initial setup
    // but could be improved with non-blocking or timeout logic in production code.
    // This is a simple implementation for the sake of this example.
    struct sockaddr_in masterAddr;
    masterAddr.sin_family = AF_INET;
    masterAddr.sin_port = htons(port);

    // Convert hostname to IP
    struct hostent* server = gethostbyname(host.c_str());
    if (!server) {
        std::cerr << "Failed to resolve host\n";
        return;
    }

    std::memcpy(&masterAddr.sin_addr.s_addr, server->h_addr, server->h_length);

    if (connect(sock, (struct sockaddr*)&masterAddr, sizeof(masterAddr)) < 0) {
        perror("Connection to master failed");
        return;
    }

    auto sendResp = [&](const std::string& resp) {
        ssize_t sent = send(sock, resp.c_str(), resp.size(), 0);
        if (sent == -1) {
            perror("Failed to send to master");
        }
    };

    auto readLineFromMaster = [&]() -> std::string {
        std::string line;
        char ch;
        while (read(sock, &ch, 1) == 1) {
            line += ch;
            if (line.size() >= 2 && line[line.size()-2] == '\r' && line[line.size()-1] == '\n') {
                break;
            }
        }
        return line;
    };

    // Step 1: Send PING
    std::string pingResp = "*1\r\n$4\r\nPING\r\n";
    sendResp(pingResp);

    std::string pong = readLineFromMaster();
    if (pong.substr(0, 4) != "+PON") {
        std::cerr << "Did not receive PONG from master: " << pong << std::endl;
        return;
    }

    std::cout << "Received: " << pong;
    std::fflush(stdout);

    // Step 2: Send REPLCONF listening-port
    std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                            std::to_string(std::to_string(::port).size()) + "\r\n" +
                            std::to_string(::port) + "\r\n";
    sendResp(replconf1);

    std::string ok1 = readLineFromMaster();
    if (ok1.substr(0, 3) != "+OK") {
        std::cerr << "Did not receive OK to REPLCONF listening-port: " << ok1 << std::endl;
        return;
    }

    std::cout << "Received: " << ok1;
    std::fflush(stdout);

    // Step 3: Send REPLCONF capa psync2
    std::string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    sendResp(replconf2);

    std::string ok2 = readLineFromMaster();
    if (ok2.substr(0, 3) != "+OK") {
        std::cerr << "Did not receive OK to REPLCONF capa psync2: " << ok2 << std::endl;
        return;
    }

    std::cout << "Received: " << ok2;

    // Step 4: Send PSYNC ? -1
    std::string psyncCmd ="*3\r\n""$5\r\nPSYNC\r\n""$1\r\n?\r\n""$2\r\n-1\r\n";
    sendResp(psyncCmd);

    // Step 6: Wait for master's full response (can be +FULLRESYNC or +CONTINUE or -ERR)
    // For now, just read and log it
    std::string psyncResponse = readLineFromMaster();
    std::cout << "Master response to PSYNC: " << psyncResponse << std::endl;

    std::fflush(stdout);

    // Keep socket open if PSYNC is to be implemented later
}


static void readBytes(std::ifstream &is, char *buffer, int length) {
    if (!is.read(buffer, length)) {
        throw std::runtime_error("Unexpected EOF when reading RDB file");
    } 
}

static uint8_t readByte(std::ifstream &is) {
    char byte;
    if (!is.read(&byte, 1)) {
        throw std::runtime_error("Unexpected EOF when reading RDB file");
    }
    return static_cast<uint8_t>(byte);
}

int readLength(std::ifstream &is, bool &isValue) {
  uint8_t firstByte = readByte(is);
  std::cout << "\n First Byte: " << std::to_string(firstByte) << "\n";

  uint8_t flag = (firstByte & 0xC0) >> 6;
  uint8_t value = firstByte & 0x3F;
  std::cout << "\n Flag & Value: " << std::to_string(flag) << " "
            << std::to_string(value) << "\n";

  if (flag == 0) {
    return value;
  } else if (flag == 1) {
    uint8_t secondByte = readByte(is);
    return value << 8 | secondByte;
  } else if (flag == 2) {
    uint8_t secondByte = readByte(is);
    uint8_t thirdByte = readByte(is);
    uint8_t fourthByte = readByte(is);
    uint8_t fifthByte = readByte(is);
    return fifthByte << 24 | fourthByte << 16 | thirdByte << 8 | secondByte;
  } else if (flag == 3) {
    isValue = true;
    if (value == 0) {
      uint8_t secondByte = readByte(is);
      return secondByte;
    } else if (value == 1) {
      uint8_t secondByte = readByte(is);
      uint8_t thirdByte = readByte(is);
      return thirdByte << 8 | secondByte;
    } else if (value == 2) {
      uint8_t secondByte = readByte(is);
      uint8_t thirdByte = readByte(is);
      uint8_t fourthByte = readByte(is);
      uint8_t fifthByte = readByte(is);
      return fifthByte << 24 | fourthByte << 16 | thirdByte << 8 | secondByte;
    }
  }
  return -1;
}

// Propagate command to replica if connected
void propagateToReplica(const std::vector<std::string>& command) 
{
    if (replicaFd != -1) {
        std::ostringstream oss;
        oss << "*" << command.size() << "\r\n";
        for (const auto& arg : command) {
            oss << "$" << arg.size() << "\r\n" << arg << "\r\n";
        }
        std::string payload = oss.str();
        send(replicaFd, payload.c_str(), payload.size(), 0);
    }
}

void loadRDBFile() {
    std::string fullPath = config_dir + "/" + config_dbfilename;
    FILE* file = fopen(fullPath.c_str(), "rdb");
    if (!file) {
        std::cerr << "RDB file not found: " << fullPath << std::endl;
        return; // File may not exist; treat as empty DB
    }

    char header[9];
    if (fread(header, 1, 9, file) != 9 || std::string(header, 5) != "REDIS") {
        std::cerr << "Invalid RDB header\n";
        fclose(file);
        return;
    }

    while (true) {
        int type = fgetc(file);
        if (type == EOF || type == 0xFF) break;  // End of file

        // Read key
        int keyLen = fgetc(file);  // 1-byte length (simple case)
        if (keyLen == EOF) break;
        char* keyBuf = new char[keyLen];
        fread(keyBuf, 1, keyLen, file);
        std::string key(keyBuf, keyLen);
        delete[] keyBuf;

        // Read string value
        int valType = type;  // type also indicates value type
        if (valType == 0) {  // 0 = string
            int valLen = fgetc(file);
            if (valLen == EOF) break;
            char* valBuf = new char[valLen];
            fread(valBuf, 1, valLen, file);
            std::string value(valBuf, valLen);
            delete[] valBuf;

            std::lock_guard<std::mutex> lock(store_mutex);
            store[key] = Entry{value, std::chrono::steady_clock::time_point::max()};
        } else {
            std::cerr << "Unsupported RDB value type\n";
        }
    }

    fclose(file);
}

// Read a full line ending with \r\n
std::string readLine(int fd) {
    std::string line;
    char ch;
    while (read(fd, &ch, 1) == 1) {
        line += ch;
        if (line.size() >= 2 && line[line.size()-2] == '\r' && line[line.size()-1] == '\n') {
            break;
        }
    }
    return line;
}

// Parse RESP command into a vector of strings
std::vector<std::string> parseRESP(int fd) {
    std::vector<std::string> result;

    std::string line = readLine(fd);
    if (line.empty() || line[0] != '*') return result;

    int numElements = std::stoi(line.substr(1));
    for (int i = 0; i < numElements; ++i) {
        std::string lenLine = readLine(fd);
        if (lenLine.empty() || lenLine[0] != '$') return result;

        int strLen = std::stoi(lenLine.substr(1));
        std::string arg;
        arg.resize(strLen);
        char ch; // <-- declare here
        read(fd, &arg[0], strLen);
        read(fd, &ch, 1); // \r
        read(fd, &ch, 1); // \n
        result.push_back(arg);
    }
    return result;
}


// Stream support
struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
};
std::unordered_map<std::string, std::vector<StreamEntry>> streams;
std::mutex streams_mutex;

// Helper to parse stream ID into (ms, seq) (no star support)
bool parseStreamId(const std::string& id, uint64_t& ms, uint64_t& seq) {
    size_t dash = id.find('-');
    if (dash == std::string::npos) return false;
    try {
        ms = std::stoull(id.substr(0, dash));
        seq = std::stoull(id.substr(dash + 1));
    } catch (...) {
        return false;
    }
    return true;
}

// Helper to parse stream ID into (ms, seq), with support for '*' as seq
bool parseStreamIdWithStar(const std::string& id, uint64_t& ms, bool& seqIsStar, uint64_t& seq) {
    size_t dash = id.find('-');
    if (dash == std::string::npos) return false;
    try {
        ms = std::stoull(id.substr(0, dash));
        if (id.substr(dash + 1) == "*") {
            seqIsStar = true;
            seq = 0;
        } else {
            seqIsStar = false;
            seq = std::stoull(id.substr(dash + 1));
        }
    } catch (...) {
        return false;
    }
    return true;
}

void handleClient(int client_fd) {
    // Transaction state per client
    thread_local bool inTransaction = false;
    thread_local std::vector<std::vector<std::string>> transactionQueue;

    // Detect if this connection is a replica connection
    bool isReplicaConn = (client_fd == replicaFd);

    while (true) {
        std::vector<std::string> command = parseRESP(client_fd);
        if (command.empty()) {
            // Clear transaction state on disconnect
            inTransaction = false;
            transactionQueue.clear();
            close(client_fd);
            return;
        }

        if (command.size() < 1) {
            std::string err = "-ERR wrong number of arguments\r\n";
            // Only send error to clients, not to replica connections
            if (!isReplicaConn) send(client_fd, err.c_str(), err.size(), 0);
            continue;
        }
        std::string cmd = command[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

        // Do not send responses to replica connections except for handshake commands
        bool suppressResponse = isReplicaConn &&
            cmd != "REPLCONF" && cmd != "PSYNC" && cmd != "PING" && cmd != "INFO" && cmd != "CONFIG";

        // Transaction queuing logic
        if (inTransaction && cmd != "EXEC" && cmd != "DISCARD") {
            transactionQueue.push_back(command);
            std::string response = "+QUEUED\r\n";
            if (!suppressResponse) send(client_fd, response.c_str(), response.size(), 0);
            continue;
        }

        if (cmd == "MULTI" && command.size() == 1) 
        {
            inTransaction = true;
            transactionQueue.clear();
            std::string response = "+OK\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "EXEC" && command.size() == 1) 
        {
            // Execute all queued commands and return RESP array of responses
            std::vector<std::string> responses;
            for (const auto& queuedCmd : transactionQueue) {
                std::string queuedCmdName = queuedCmd[0];
                std::transform(queuedCmdName.begin(), queuedCmdName.end(), queuedCmdName.begin(), ::toupper);

                // Only support GET, SET, INCR, ECHO for simplicity
                if (queuedCmdName == "SET" && queuedCmd.size() >= 3) {
                    std::string key = queuedCmd[1];
                    std::string value = queuedCmd[2];
                    std::chrono::steady_clock::time_point expireAt = std::chrono::steady_clock::time_point::max();
                    if (queuedCmd.size() >= 5 && (queuedCmd[3] == "PX" || queuedCmd[3] == "px")) {
                        int ttl_ms = std::stoi(queuedCmd[4]);
                        expireAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms);
                    }
                    {
                        std::lock_guard<std::mutex> lock(store_mutex);
                        store[key] = Entry{value, expireAt};
                    }
                    responses.push_back("+OK\r\n");
                } else if (queuedCmdName == "GET" && queuedCmd.size() >= 2) {
                    std::string key = queuedCmd[1];
                    std::string resp = "$-1\r\n";
                    {
                        std::lock_guard<std::mutex> lock(store_mutex);
                        auto it = store.find(key);
                        if (it != store.end()) {
                            auto now = std::chrono::steady_clock::now();
                            if (it->second.expireAt.time_since_epoch().count() != 0 && now > it->second.expireAt) {
                                store.erase(it);
                            } else {
                                const std::string& value = it->second.value;
                                resp = "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
                            }
                        }
                    }
                    responses.push_back(resp);
                } else if (queuedCmdName == "INCR" && queuedCmd.size() >= 2) {
                    std::string key = queuedCmd[1];
                    std::string resp;
                    {
                        std::lock_guard<std::mutex> lock(store_mutex);
                        auto it = store.find(key);
                        if (it != store.end()) {
                            try {
                                int value = std::stoi(it->second.value);
                                value++;
                                it->second.value = std::to_string(value);
                                resp = ":" + std::to_string(value) + "\r\n";
                            } catch (const std::invalid_argument&) {
                                resp = "-ERR value is not an integer or out of range\r\n";
                            }
                        } else {
                            store[key] = Entry{"1", std::chrono::steady_clock::time_point::max()};
                            resp = ":1\r\n";
                        }
                    }
                    responses.push_back(resp);
                } else if (queuedCmdName == "ECHO" && queuedCmd.size() >= 2) {
                    std::string resp = "$" + std::to_string(queuedCmd[1].size()) + "\r\n" + queuedCmd[1] + "\r\n";
                    responses.push_back(resp);
                } else {
                    responses.push_back("-ERR unknown command\r\n");
                }
            }
            // RESP array of responses
            std::ostringstream oss;
            oss << "*" << responses.size() << "\r\n";
            for (const auto& resp : responses) {
                oss << resp;
            }
            send(client_fd, oss.str().c_str(), oss.str().size(), 0);
            // Clear transaction state
            inTransaction = false;
            transactionQueue.clear();
        }
        else if (cmd == "PING"){
            std::string response = "+PONG\r\n";
            send(client_fd, response.c_str(),response.size(), 0);
        }
        else if (cmd == "REPLCONF") {
            std::string response = "+OK\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "PSYNC" && command.size() == 3) {
            std::string response = "+FULLRESYNC " + master_replid + " " + std::to_string(master_repl_offset) + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);


             // Send RDB file as RESP Bulk String
            const std::uint8_t emptyRDB[] = {
                'R','E','D','I','S','0','0','0','9',
                0xFF,
                0x24,0x9f,0x3b,0x8b,0x88,0x35,0x90,0xcb
            };
            const size_t rdbLength = sizeof(emptyRDB);

            // Send RESP Bulk String header
            std::string bulkHeader = "$" + std::to_string(rdbLength) + "\r\n";
            send(client_fd, bulkHeader.c_str(), bulkHeader.size(), 0);

            //Send raw RDB bytes
            send(client_fd, reinterpret_cast<const char*>(emptyRDB), rdbLength, 0);

        }

        else if (cmd == "PSYNC" && command[1] == "?" && command[2] == "-1") {
            //Respond with FULLRESYNC
            std::string replId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            std::string response = "+FULLRESYNC " + replId + " 0\r\n";
            send(client_fd, response.c_str(), response.size(), 0);

            // Send RDB file (hardcoded empty)
            const std::string rdbData = "\x52\x45\x44\x49\x53\x30\x30\x30\x39\xff\x24\x9f\x3b\x8b\x88\x35\x90\xcb";
            std::string rdbPayload = "$" + std::to_string(rdbData.size()) + "\r\n" + rdbData;
            send(client_fd, rdbPayload.c_str(), rdbPayload.size(), 0);

            // ✅ Mark this connection as the replica
            replicaFd = client_fd;
        }
        else if (cmd == "SET" && command.size() >= 3) 
        {
            std::string key = command[1];
            std::string value = command[2];
            std::chrono::steady_clock::time_point expireAt = std::chrono::steady_clock::time_point::max();

            // Optional PX
            if (command.size() >= 5 && (command[3] == "PX" || command[3] == "px")) 
            {
                int ttl_ms = std::stoi(command[4]);
                expireAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms);
            }
            std::lock_guard<std::mutex> lock(store_mutex);
            store[key] = Entry{value, expireAt};

            // RESP OK response
            std::string response = "+OK\r\n";
            send(client_fd, response.c_str(), response.size(), 0);

            // Propagate to replica
            propagateToReplica(command);
        }
        else if (cmd == "ECHO" && command.size() >= 2) {
            std::string response = "$" + std::to_string(command[1].size()) + "\r\n" + command[1] + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if(cmd == "INCR" && command.size() >= 2) 
        {
            std::string key = command[1];
            std::lock_guard<std::mutex> lock(store_mutex);
            auto it = store.find(key);
            int value = 1;
            if (it != store.end()) {
                try {
                    value = std::stoi(it->second.value);
                    value++;
                    it->second.value = std::to_string(value);
                } catch (const std::invalid_argument&) {
                    std::string err = "-ERR value is not an integer or out of range\r\n";
                    send(client_fd, err.c_str(), err.size(), 0);
                    return;
                }
            } else {
                store[key] = Entry{"1", std::chrono::steady_clock::time_point::max()};
            }
            std::string response = ":" + std::to_string(value) + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);

            // Propagate to replica
            propagateToReplica(command);
        }
        else if(cmd == "GET" && command.size() >= 2)
        {
            std::string key = command[1];
            std::string response = "$-1\r\n";

            std::lock_guard<std::mutex> lock(store_mutex);
            auto it = store.find(key);
            if (it != store.end()) {
                auto now = std::chrono::steady_clock::now();
                if (it->second.expireAt.time_since_epoch().count() != 0 && now > it->second.expireAt)
                {
                    store.erase(it);
                }
                else {
                    const std::string& value = it->second.value;
                    response = "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
                }
            }
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "MULTI" && command.size() == 1) 
        {
            // RESP MULTI response
            std::string response = "+OK\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "EXEC" && command.size() == 1) 
        {
            // RESP EXEC response
            std::string response = "*0\r\n"; // No commands executed
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "QUIT" && command.size() == 1) 
        {
            close(client_fd);
            return;
        }
        else if (cmd == "CONFIG" && command.size() >= 3 && command[1] == "GET") {
            std::vector<std::pair<std::string, std::string>> matches;

            const std::string& pattern = command[2];
            if (pattern == "*" || pattern == "dir") {
                matches.emplace_back("dir", config_dir);
            }
            if (pattern == "*" || pattern == "dbfilename") {
                matches.emplace_back("dbfilename", config_dbfilename);
            }
            std::string response = "*" + std::to_string(matches.size() * 2) + "\r\n";
            for (const auto& [key, value] : matches) {
                response += "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
                response += "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
            }       
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "KEYS" && command.size() == 2 && command[1] == "*") 
        {
            // assume that "*" is passed in.

            // read the file
            std::ifstream is(config_dir + "/" + config_dbfilename);

            // identify keys segment

            // skip header section
            char header[9];
            is.read(header, 9);
            std::unordered_map<std::string, std::string> keyValue;

            // process segments
            while (true) {
                uint8_t opcode = readByte(is);
                std::cout << "\nOpcode: " << std::to_string(opcode) << "\n";
                // metadata section
                if (opcode == 0xFA) {
                    bool isValue = false;
                    int length = readLength(is, isValue);
                    std::cout << "\n Metadata Name Length: " << length << "\n";
                    char name[length];
                    is.read(name, length);
                    std::cout << "\nMetadata Name: " << name << "\n";

                    isValue = false;
                    length = readLength(is, isValue);
                    std::cout << "\n Metadata Value Length: " << length << "\n";
                    std::string value;
                    if (isValue) {
                        value = length;
                    } else {
                        is.read(&value[0], length);
                    }
                    std::cout << "\nMetadata Value: " << value << "\n";
                } else if (opcode == 0xFE) {
                    bool isValue = false;
                    int databaseIndex = readLength(is, isValue);
                } else if (opcode == 0xFB) {
                    bool isValue = false;
                    int keyValueHashSize = readLength(is, isValue);

                    isValue = false;
                    int expiryHashSize = readLength(is, isValue);
                } else if (opcode == 0x00) {
                    std::cout << "\nThis ran!\n";

                    bool isValue = false;
                    int length = readLength(is, isValue);
                    std::string key(length, '\0');
                    is.read(&key[0], length);

                    isValue = false;
                    length = readLength(is, isValue);
                    std::string val(length, '\0');
                    is.read(&val[0], length);

                    keyValue[key] = val;
                } else if (opcode == 0xFC) {
                    unsigned long time = 0;
                    for (int i = 0; i < 8; i++) {
                        uint8_t byte = readByte(is);
                        time <<= 8;
                        time |= byte;
                }
                } else if (opcode == 0xFD) {
                    unsigned int time = 0;
                    for (int i = 0; i < 4; i++) {
                    uint8_t byte = readByte(is);
                    time <<= 8;
                    time |= byte;
                }
                } else if (opcode == 0xFF) {
                    char checksum[8];
                    readBytes(is, checksum, 8);
                    break;
                }
          }
          // pull that out
          std::string response = "*" + std::to_string(keyValue.size()) + "\r\n";

          for (auto elem : keyValue) {
            std::string key = elem.first;
            response +=
                "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
          }

          send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "INFO" && command.size() >= 2 && command[1] == "replication") 
        {
            std::string response;
            if(isReplica == "")
            {  
                std::string response_string = "role:master\r\nmaster_replid:" + master_replid + "\r\nmaster_repl_offset:" + std::to_string(master_repl_offset);
                response = "$" + std::to_string(response_string.size()) + "\r\n" + response_string + "\r\n";
            }
            else {
                response = "$10\r\nrole:slave\r\n";
            }  
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if(cmd == "INFO" && command.size() >= 2)
        {
            std::string response = "$11\r\nrole:master\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }    
        else if(cmd == "TYPE" && command.size() >= 2) 
        {
            std::string key = command[1];
            std::string response = "$4\r\nnone\r\n";

            std::lock_guard<std::mutex> lock(store_mutex);
            auto it = store.find(key);
            if (it != store.end()) {
                // For simplicity, we assume all values are strings
                response = "$6\r\nstring\r\n";
            } else {
                std::lock_guard<std::mutex> stream_lock(streams_mutex);
                if (streams.find(key) != streams.end()) {
                    response = "$6\r\nstream\r\n";
                }
            }
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else if (cmd == "XADD" && command.size() >= 5) 
        {
            std::string key = command[1];
            std::string id = command[2];

            // Parse field-value pairs
            if ((command.size() - 3) % 2 != 0) {
                std::string err = "-ERR wrong number of field-value arguments\r\n";
                send(client_fd, err.c_str(), err.size(), 0);
                continue;
            }

            uint64_t ms = 0, seq = 0;
            bool seqIsStar = false;
            if (id == "*") {
                std::string err = "-ERR only explicit time part supported in this stage\r\n";
                send(client_fd, err.c_str(), err.size(), 0);
                continue;
            }
            if (!parseStreamId(id, ms, seq)) {
                std::string err = "-ERR invalid stream ID format\r\n";
                send(client_fd, err.c_str(), err.size(), 0);
                continue;
            }
            // Minimum allowed ID is 0-1
            if (!seqIsStar && ms == 0 && seq == 0) {
                std::string err = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                send(client_fd, err.c_str(), err.size(), 0);
                continue;
            }

            std::vector<std::pair<std::string, std::string>> fields;
            for (size_t i = 3; i + 1 < command.size(); i += 2) {
                fields.emplace_back(command[i], command[i+1]);
            }

            std::string finalId;
            {
                std::lock_guard<std::mutex> lock(streams_mutex);
                auto& entries = streams[key];
                uint64_t newSeq = seq;
                if (seqIsStar) {
                    // Find last entry with ms == ms, get its seq
                    newSeq = 0;
                    for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
                        uint64_t entryMs, entrySeq;
                        if (parseStreamId(it->id, entryMs, entrySeq) && entryMs == ms) {
                            newSeq = entrySeq + 1;
                            break;
                        }
                    }
                    // If no entry with ms found, default seq
                    if (ms == 0 && newSeq == 0) newSeq = 1;
                    // If ms==0 and seq==0, error (0-0 not allowed)
                    if (ms == 0 && newSeq == 0) {
                        std::string err = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                        send(client_fd, err.c_str(), err.size(), 0);
                        continue;
                    }
                }
                finalId = std::to_string(ms) + "-" + std::to_string(newSeq);

                // Validate ID is greater than last entry
                if (!entries.empty()) {
                    uint64_t lastMs, lastSeq;
                    if (!parseStreamId(entries.back().id, lastMs, lastSeq)) {
                        std::string err = "-ERR internal stream ID error\r\n";
                        send(client_fd, err.c_str(), err.size(), 0);
                        continue;
                    }
                    if (ms < lastMs || (ms == lastMs && newSeq <= lastSeq)) {
                        std::string err = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        send(client_fd, err.c_str(), err.size(), 0);
                        continue;
                    }
                }
                entries.push_back(StreamEntry{finalId, fields});
            }

            std::string response = "$" + std::to_string(finalId.size()) + "\r\n" + finalId + "\r\n";
            if (!suppressResponse) send(client_fd, response.c_str(), response.size(), 0);
            propagateToReplica(command);
        }
    }
}



int main(int argc, char* argv[]) {

    for(int i = 1; i < argc; i++){
        std::string arg = argv[i];
        if(arg == "--dir"){
            config_dir = argv[i + 1];
            i++;
        }
        else if (arg == "--dbfilename"){
            config_dbfilename = argv[i + 1];
            i++;
        }
        else if (arg == "--port"){
            port = std::stoi(argv[i + 1]);
            i++;
        }
        else if(arg == "--replicaof"){
            isReplica = argv[i + 1];
            i++;
            size_t pos = isReplica.find(' ');
            if (pos != std::string::npos) {
                replicaHost = isReplica.substr(0, pos);
                replicaPort = std::stoi(isReplica.substr(pos + 1));
            }
        }
    }
    //CLI flag parsing 

    if (!replicaHost.empty()) {
        std::thread(connectToMasterAndSendPing, replicaHost, replicaPort).detach();
    }

   loadRDBFile();
    //Not shared across threads, so no need to worry of synchronizing it 
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);

    while (true) {
        sockaddr_in client_addr {};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_len);
        std::thread(handleClient, client_fd).detach();
    }

    close(server_fd);
    return 0;
}


// Note: This code is a simplified Redis-like server implementation in C++.
// It does not handle all Redis commands or features, but provides a basic structure
// for handling commands like SET, GET, INCR, ECHO, MULTI/EXEC

// Summary:
// This file implements a minimal Redis-like server in C++ supporting basic commands (SET, GET, INCR, ECHO, MULTI/EXEC, TYPE, KEYS, CONFIG, PING, QUIT), simple replication, and partial stream (XADD) support.
// - Uses TCP sockets for networking and threads for concurrency.
// - Maintains an in-memory key-value store with optional expiry.
// - Supports Redis streams with explicit and partial auto-generated IDs for XADD.
// - Handles replication by propagating commands to a replica over a dedicated connection, suppressing responses to the replica except for handshake commands.
// - RESP protocol parsing and serialization is implemented for communication.
// - RDB file loading is stubbed for persistence simulation.
// - Command handling is done in a single function with per-connection state for transactions.
