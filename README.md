Requirements for the extended microservice-based system:

Start cloning git@gitlab.inf.unibz.it:andrea-janes/cse-microservices3.git (git clone git@gitlab.inf.unibz.it:andrea-janes/cse-microservices3.git in some directory of your choice)
Extend the appartment service so that you can
specify the size when you add an apparment (the command becomes /add?name=...&size=...)
delete an appartment (the command becomes /remove?name=...)
Add a "reserve" microservice that allows to
add a reservation (the command is /add?name=...&start=yyyymmdd&duration=...&vip=1)
remove a reservation (the command is /remove?id=...)
Extend the search service so that you can 
Search free appartments using the command /search?date=...&duration=...
Add a "gateway" microservice that forwards these five commands to the right microservices:
/appartments/add
/appartments/remove
/search
/reserve/add
/reserve/remove
To implement this, you need to:

Add the "squarmeter" column to the table appartment in the appartment service db
Create the "reserve" microservice, subscribe to AppartmentAdded and AppartmentRemoved events and synchronized the data
Extend the "search" service so that it also listens to the AppartmentRemoved events and also the ReservationAdded and ReservationRemoved events.