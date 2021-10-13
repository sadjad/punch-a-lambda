#include "LocalStorage.hpp"

LocalStorage::LocalStorage(size_t max_size) : max_size_(max_size), total_size_(0)
{}

int LocalStorage::get_total_size()
{
    return total_size_;
}

std::optional <Blob> LocalStorage::locate(std::string key)
{
    std::unordered_map<std::string,Blob>::iterator got = storage_.find (key);
    if(got == storage_.end())
    {
        auto alias_got = alias_.find(key);
        if(alias_got != alias_.end())
        {
            return storage_.find(alias_got->second)->second;
        }
        else
        {
            return {};
        }      
    } else{
        return got->second;
    }
}

std::optional <void *> LocalStorage::new_object(std::string key, size_t size)
{
    if (total_size_ + size > max_size_)
    {
        std::cerr << "Allocation surpassing maximum size" << std::endl;
        return {};
    } else
    {
        void * ptr = malloc(size);
        total_size_ += size;

        if (alias_.find(key) != alias_.end())
        {
            std::cerr << "key is in aliases" << std::endl;
            return {};
        }

        bool ok = storage_.insert({key, {true, size, ptr}}).second;
        if(ok)
        {
            key2alias_.insert({key,{}});
            return ptr;
        } else
        {
            std::cerr << "key is in storage" << std::endl;
            return {};
        }
    }
}

int LocalStorage::new_object_from_string(std::string key, std::string object)
{
    auto size = object.length();
    if (total_size_ + size > max_size_)
    {
        std::cerr << "Allocation surpassing maximum size" << std::endl;
        return 0;
    } else
    {
        void * ptr = malloc(size);
        std::memcpy(ptr, object.c_str(), size);
        total_size_ += size;

        if (alias_.find(key) != alias_.end())
        {
            std::cerr << "key is in aliases" << std::endl;
            return 0;
        }

        bool ok = storage_.insert({key, {true, size, ptr}}).second;
        if(ok)
        {
            key2alias_.insert({key,{}});
            return 1;
        } else
        {
            std::cerr << "key is in storage" << std::endl;
            return 0;
        }
    }
}


int LocalStorage::commit(std::string key)
{
    auto alias_lookup = alias_.find(key);
    if ( alias_lookup != alias_.end())
    {
        auto real_key = alias_lookup->second;
        storage_.find(real_key)->second.mutablility = false;
        return 0;
    } else 
    {
        auto storage_lookup = storage_.find(key);
        if (storage_lookup != storage_.end())
        {
            storage_lookup->second.mutablility = false;
            return 0;
        } else
        {
            std::cerr << "commit key not found" << std::endl;
            return 1;
        }
    }
}

int LocalStorage::grow(std::string key, size_t size)
{
    if(size + total_size_ > max_size_)
    {
        std::cerr << "out of memory" << std::endl;
        return 1;
    }
    auto alias_lookup = alias_.find(key);
    if ( alias_lookup != alias_.end())
    {
        auto real_key = alias_lookup->second;
        Blob & blob = storage_.find(real_key)->second;
        if(blob.mutablility == false)
        {
            std::cerr << "cannot grow an immutable blob" << std::endl;
            return 1;
        }
        blob.size += size;
        blob.ptr = realloc(blob.ptr, blob.size);
        return 0;
    } else 
    {
        auto storage_lookup = storage_.find(key);
        if (storage_lookup != storage_.end())
        {
            Blob & blob = storage_lookup->second;
            if(blob.mutablility == false)
            {
                std::cerr << "cannot grow an immutable blob" << std::endl;
                return 1;
            }
            blob.size += size;
            blob.ptr = realloc(blob.ptr, blob.size);
            return 0;
        } else
        {
            std::cerr << "commit key not found" << std::endl;
            return 1;
        }
    }
}

int LocalStorage::delete_object(std::string key)
{
    auto alias_lookup = alias_.find(key);
    if ( alias_lookup != alias_.end())
    {
        auto real_key = alias_lookup->second;
        free(storage_.find(real_key)->second.ptr);
        total_size_ -= storage_.find(real_key)->second.size;
        storage_.erase(real_key);
        // now go ahead and remove all the aliases too
        auto aliases = key2alias_.find(real_key)->second;
        for(auto alias : aliases)
        {
            alias_.erase(alias);
        }
        key2alias_.erase(real_key);
        return 0;
    } else 
    {
        auto storage_lookup = storage_.find(key);
        if (storage_lookup != storage_.end())
        {
            free(storage_.find(key)->second.ptr);
            total_size_ -= storage_.find(key)->second.size;
            storage_.erase(key);
            return 0;
        } else
        {
            std::cerr << "commit key not found" << std::endl;
            return 1;
        }

    }
}

//figure out what happens if it's actually an update
int LocalStorage::add(std::string key, std::string alias)
{
    if(storage_.find(key) == storage_.end())
    {
        std::cerr << "key not in storage" << std::endl;
        return 1;
    } else {
        auto got = alias_.find(alias);
        if(got != alias_.end())
        {
            if(got->second.compare(key) == 0)
            {
                return 0;
            } else {
                std::cerr << "update is not supported" << std::endl;
                return 1;
            }
            
        }
        alias_.insert({alias,key});
        key2alias_.find(key)->second.push_back(alias);
        return 0;
    }
}