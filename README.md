# bgworker - a Python background worker for Cisco NSO
bgworker is an NSO package that shows how we can implement a background worker
process in Python for Cisco NSO.

While this comes as a complete NSO package which has an example background
worker function that is executed, the idea is that you will take the
background_process.py file and incorporate into your own NSO package when you
need to launch a background worker process. See the example use in
python/bgworker/main.py on how to use it.

It handles a number of things that aren't otherwise entirely intuitive how to
handle, such as:
 - NCS package events like reload and redeploy
 - background worker process dying (will restart it)
 - configuration changes - enabling/disabling of the background worker
 - HA events - only run background process when HA master or HA is disabled

NSO doesn't have a natural way of shipping an NSO package whose Python files
should just be available to other packages, which is why you simply have to copy
the background_process.py file. It could potentially be placed on pip but it
seems to specific (to NSO) to be worth putting there, at least for now. Let me
know if you feel otherwise.
