#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <vector>
#include <map>
#include <curl/curl.h>
#include <json/json.h>

bool IsEnglishTitle(const std::string& title) {
    // This is a simple check based on printable ASCII characters
    for (char ch : title) {
        if (ch < 32 || ch > 126) {
            return false;
        }
    }
    return true;
}


// Replace "YOUR_API_KEY" with your TMDB API key
const std::string API_KEY = "YOUR_API_KEY";
const std::string BASE_URL = "https://api.themoviedb.org/3";

// Function to perform HTTP GET request
size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append(static_cast<char*>(contents), totalSize);
    return totalSize;
}

// Function to join strings in a vector with a delimiter
std::string JoinStrings(const std::vector<std::string>& strings, const std::string& delimiter) {
    std::string result;
    for (size_t i = 0; i < strings.size(); ++i) {
        result += strings[i];
        if (i < strings.size() - 1) {
            result += delimiter;
        }
    }
    return result;
}

struct Movie {
    int id;
    double popularity;
    std::string original_title;
    std::vector<std::string> cast;
    std::string overview;
    int vote_count;
    double vote_average;
    int release_year;
    std::vector<std::string> genres; // Added genres field
};



// Function to perform HTTP GET request
std::string PerformHttpRequest(const std::string& url) {
    CURL* curl = curl_easy_init();
    std::string response;

    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        CURLcode res = curl_easy_perform(curl);

        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            std::cerr << "Failed to perform HTTP request: " << curl_easy_strerror(res) << std::endl;
        }
    }

    return response;
}

// Function to fetch movie details for a given year and page
std::vector<Movie> FetchMovieDetails(int year, int page) {
    std::vector<Movie> movies;

    // Fetch genre names from the TMDB API
    std::string genresUrl = BASE_URL + "/genre/movie/list?api_key=" + API_KEY;
    std::string genresResponse = PerformHttpRequest(genresUrl);

    Json::CharReaderBuilder genresReaderBuilder;
    Json::Value genresRoot;
    std::istringstream genresJsonStream(genresResponse);

    std::map<int, std::string> genreMap;  // Map to store genre IDs and names

    if (Json::parseFromStream(genresReaderBuilder, genresJsonStream, &genresRoot, nullptr)) {
        for (const auto& genre : genresRoot["genres"]) {
            int genreId = genre["id"].asInt();
            std::string genreName = genre["name"].asString();
            genreMap[genreId] = genreName;
        }
    } else {
        std::cerr << "Failed to parse genres JSON response." << std::endl;
        return movies;  // Return an empty vector in case of failure
    }

    // Fetch movie details using TMDB API
    std::string url = BASE_URL + "/discover/movie?api_key=" + API_KEY + "&primary_release_year=" + std::to_string(year) + "&page=" + std::to_string(page);
    std::string response = PerformHttpRequest(url);

    Json::CharReaderBuilder readerBuilder;
    Json::Value root;
    std::istringstream jsonStream(response);

    if (Json::parseFromStream(readerBuilder, jsonStream, &root, nullptr)) {
        for (const auto& movie : root["results"]) {
            try {
                Movie movieDetails;
                movieDetails.id = movie["id"].asInt();
                movieDetails.popularity = movie["popularity"].asDouble();
                movieDetails.original_title = movie["original_title"].asString();

                // Skip movies whose "original_title" is not in English
                if (!IsEnglishTitle(movieDetails.original_title)) {
                    continue;
                }

                // Fetch genres for the movie
                for (const auto& genreId : movie["genre_ids"]) {
                    int id = genreId.asInt();
                    auto it = genreMap.find(id);
                    if (it != genreMap.end()) {
                        movieDetails.genres.push_back(it->second);
                    }
                }

                // Skip movies without genres
                if (movieDetails.genres.empty()) {
                    continue;
                }

                // Fetch credits to get the cast information
                int movieId = movieDetails.id;
                std::string creditsUrl = BASE_URL + "/movie/" + std::to_string(movieId) + "/credits?api_key=" + API_KEY;
                std::string creditsResponse = PerformHttpRequest(creditsUrl);

                Json::Value creditsRoot;
                std::istringstream creditsJsonStream(creditsResponse);

                if (Json::parseFromStream(readerBuilder, creditsJsonStream, &creditsRoot, nullptr)) {
                    for (const auto& actor : creditsRoot["cast"]) {
                        movieDetails.cast.push_back(actor["name"].asString());
                    }
                }

                // Fetch other movie details
                movieDetails.overview = movie["overview"].asString();
                movieDetails.vote_count = movie["vote_count"].asInt();
                movieDetails.vote_average = movie["vote_average"].asDouble();
                // Extracting release_year from the release_date
                movieDetails.release_year = std::stoi(movie["release_date"].asString().substr(0, 4));

                movies.push_back(movieDetails);
            } catch (const std::exception& e) {
                std::cerr << "Error processing a movie record: " << e.what() << std::endl;
                // You can choose to log the error or handle it in other ways
            }
        }
    } else {
        std::cerr << "Failed to parse JSON response." << std::endl;
    }

    return movies;
}


void WriteToCSV(const std::vector<Movie>& movies, const std::string& filename, bool includeHeader) {
    std::ofstream csvFile(filename, std::ios::app);  // Open the file in append mode

    // Write the header only if it's the first page
    if (includeHeader && csvFile.tellp() == 0) {
        csvFile << "id,popularity,original_title,cast,overview,vote_count,vote_average,release_year,genres" << std::endl;
    }

    // Write data
    for (const auto& movie : movies) {
        // Exclude records where vote_count is zero or genres are not present
        if (movie.vote_count > 0 && !movie.genres.empty()) {
            csvFile << movie.id << ","
                    << std::fixed << std::setprecision(6) << movie.popularity << ","
                    << "\"" << movie.original_title << "\","
                    << "\"" << JoinStrings(movie.cast, ", ") << "\","
                    << "\"" << movie.overview << "\","
                    << movie.vote_count << ","
                    << std::fixed << std::setprecision(2) << movie.vote_average << ","
                    << movie.release_year << ","
                    << "\"" << JoinStrings(movie.genres, ", ") << "\"" << std::endl;
        }
    }

    csvFile.close();
}

int main() {
    // Open the CSV file and write the header
    std::ofstream csvFile("movies_all_years.csv");
    csvFile << "id,popularity,original_title,cast,overview,vote_count,vote_average,release_year,genres" << std::endl;
    csvFile.close();

    // Fetch movie details for each year between 2000 and 2020
    for (int year = 2000; year <= 2020; ++year) {
        int page = 1;
        std::vector<Movie> pageMovies;

        do {
            pageMovies = FetchMovieDetails(year, page);
            // Append data to CSV file for the current page
            WriteToCSV(pageMovies, "movies_all_years.csv", page == 1); // Include header only for the first page
            ++page;
        } while (!pageMovies.empty());
    }

    return 0;
}