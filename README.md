# dbfstuff
Read/convert DBF-files with Rust
## What does it do?
Command line tool that converts every dbf-file in the given directory into a csv-file. Contents of memofields (from fpt-/dbt-files) are being included.
## How did it came to be?
At work I had a set of dbf-files that weren't working with my standard dbf software (turns out the offset in the field definitions was missing). I was in the process of learning Rust, so I took this as my project.

I also couldn't find an existing converter. Most that I could find were online tools and uploading my files was not an option.
## What's the status?
It does what I needed it to do.

The next steps would be to clean it up and do a Rust-worthy error handling. But since it already served its purpose, there is a certain lack of motivation.
## Known issues
I hope your files are in standard encoding. After chosing the encoding library and using it for the normal case (Windows-1252), it turned out all the other encodings, that are possible in dbf, are not supported.

Look in the code comments or in the issues for more.
