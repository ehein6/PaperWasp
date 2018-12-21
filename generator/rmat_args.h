#pragma once
#include <cinttypes>
#include <sstream>

struct rmat_args
{
    double a, b, c, d;
    int64_t num_edges, num_vertices;

    static int64_t
    parse_int_with_suffix(std::string token)
    {
        int64_t n = static_cast<int64_t>(std::stoll(token));
        switch(token.back())
        {
            case 'K': n *= 1LL << 10; break;
            case 'M': n *= 1LL << 20; break;
            case 'G': n *= 1LL << 30; break;
            case 'T': n *= 1LL << 40; break;
            default: break;
        }
        return n;
    }

    static rmat_args
    from_string(std::string str)
    {
        std::istringstream ss(str);

        rmat_args args;
        std::string token;
        // Format is a-b-c-d-ne-nv.rmat
        // Example: 0.55-0.15-0.15-0.15-500M-1M.rmat
        std::getline(ss, token, '-'); args.a = std::stod(token);
        std::getline(ss, token, '-'); args.b = std::stod(token);
        std::getline(ss, token, '-'); args.c = std::stod(token);
        std::getline(ss, token, '-'); args.d = std::stod(token);
        std::getline(ss, token, '-'); args.num_edges = parse_int_with_suffix(token);
        std::getline(ss, token, '.'); args.num_vertices = parse_int_with_suffix(token);

        return args;
    }

    std::string
    validate() const {
        std::ostringstream oss;
        // Validate parameters
        if (a < 0 || b < 0 || c < 0 || d < 0
        ||  a > 1 || b > 1 || c > 1 || d > 1
        ||  a + b + c + d != 1.0)
        {
            oss << "Invalid arguments: RMAT parameters must be fall in the range [0, 1] and sum to 1\n";
        } else if (num_edges < 0 || num_vertices < 0) {
            oss << "Invalid arguments: RMAT graph must have a positive number of edges and vertices\n";
        }
        return oss.str();
    }
};
