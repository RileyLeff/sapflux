# Sapflux

This is the sapflux data processing pipeline.

Work in progress.

The main thing that needs to be documented here is my ridiculous design decision to bundle 4 copies of the chrome webdriver into the repository.

Let me explain myself briefly:

- we want to (ab)use google drive as a live database
- we want to build & process the data from google drive using a github actions instance
- that github action should occasionally check for updates to the google drive folder, download a new copy of the dataset if appropriate, then process it using some of the R functions in /exec/.
- we don't want to version control the very large # of data files that get spat out of our loggers
- imo it's too big of a file size (several hundred MB) to make git tracking reasonable
- and i don't want to have to explain to downstream users about git pull to synchronize state 1000x. it's not the responsibility of this repository to teach people (likely researchers that are unfamiliar with version control) how to use git.
- directly downloading the data using the google drive API is extremely inefficient
- google drive API only allows for sequential file downloads, can't zip & compress server-side via the API (reasonable, probably designed to prevent use-cases like mine)
- takes 30-60 minutes to download the dataset Sundar's way
- troubleshooting that process when it inevitably breaks in the future would be intractable
- but downloading the dataset via point and click on google drive web GUI only takes ~90 seconds to download on my university's wifi, because there's some zip and compress action going on behind the scenes
- thinky_face.jpeg
- we use selenium + a platform-specific instance of chromedriver to pretend that we are manually clicking download on the dataset in googledrive GUI
- this is somewhat lower-cost in terms of storage space than directly storing the files
- this is much faster than downloading directly via API
- eat your heart out, Sundar Pichai