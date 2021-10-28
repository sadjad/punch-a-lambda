#pragma once

#include <fstream>
#include <map>
#include <string>

std::map<size_t, std::string> get_peer_addresses( const uint32_t thread_id,
                                        const std::string& master_ip,
                                        const uint16_t master_port,
                                        const uint32_t block_dim, 
                                        std::ofstream & fout);