# A visual exploration of the Marvel Cinematic Universe's timeline.

This dataviz will display all the important meetings and main events of the MCU's Infinity Saga and to highlight the interations between the characters involved in those events. At the time of writing, the Saga is considered completed and consists of 23 movies, from 2008's _Iron Man_ to 2019's _Spiderman: Far From Home_, set briefly after the events of _Avengers: Endgame_.

Originally from an idea of [tomception](https://visual.ly/users/tomception/portfolio), author of the following works:
- [Marvel Timering](https://visual.ly/community/Infographics/entertainment/marvel-timering) (april  2019)
- [Avengers Full Timeline](https://visual.ly/community/Infographics/entertainment/avengers-full-timeline) (may 2019, post _Avengers: Endgame_)


Data for this visualization has been manually compiled from the above works and integrated with details from the [Marvel Cinematic Universe Wiki Timeline](https://marvelcinematicuniverse.fandom.com/wiki/Timeline).

## Main goals & concepts

- Expand the concept explored in the above works (include more events from the MCU fan wiki)
- Improve readability by using an horizontal timeline
- Offer actual interactivity, with zooming, panning, focus, etc.

There are two main concepts: **Events** and **Characters**.

An Event is a pivotal point in one movies' plot (usually a meeting between 2+ characters, a fight, an interaction with plot items, etc). Each event has a **Date**, a brief **Description** and a list of involved **Characters**; optional attributes are a **Title** (for memorable events like the _Battle of New York_) and the **Movie** in which the event is shown; we don't consider movie scenes in which the event is just mentioned. Events are "dots" arranged on the time axis in chronological order. Important events, with lots of characters, are drawn bigger (thus, the event's radius is proportional to the # of characters).

A Character may be an actual living character or an important object (like the _Infinity Stones_) and is represented by an icon and a unique color scheme. Its icon appears near each event in which it is involved. A line with the character's color connects all its events. 

## Interaction 

A user will be able to navigate the timeline in different ways. 
Basic interactions include **zooming** (scale the space between the events), **panning** (along the time axis) and **hovering** (on an event: show the description/title, on a character line: show the character's name and highlight the line).

- Clicking on an Event will show  popup panel recapping all its information.
- Clicking on a Character's line or icon, the [<] [>] navigation arrows will appear and the user will be able to navigate that character's story.
- A list of the Saga's movies will be available for selection. Clicking on a movie will focus on all the events related to that movie.


## Technical goals & challenges

> Work-in-progress: this is a personal roadmap of all the technical challenges I'll have to overcome. 

#### 1. How to arrange the events on the Y-axis?

A Character line is the union of more events (points). Each Character starts its adventure from an origin point, from which its line originates. Lines should always be parallel to the X-axis and _not_ overlap

Easy scenario: if one or more events involve one character, events have the same Y-coordinate.

    (e0)-->--(e1)-->--(e2)

- Hard scenario: **What if the event involves more characters?** Each line occupies a "slot" corresponding to a given Y-coordinate from its X-coordinate to the next event involving that character. The event is placed "between" the two lines at a mediane Y-coordinate. The lines then converge to the event's center.
- Each change of the Y-value should happen in a steep ramp in an short time (aka X) interval. **How wide should the interval be?**

To actually draw the slope, add some dummy points before and after the event:


    (oA)----->----(*) \     / (*) ---->
                       (eAB)
    (oB)----->----(*) /     \ (*) ----> 

**Y-space filling:** let oA and oB the origin events of characters A and B. oA is at x=0, oB is at x=2. oA is drawn closer to the X-axis (y=0), while oB is drawn further, above or below oB. 

- Another solution is to reserve a slice of the Y-axis for a given film series (like all _Captain America_ movies), but **where do we put the events without a movie?**

**Grouping**: if 2+ characters are "together" in the time occurring between two events, their lines will be joined together (for example, the _Guardians of the Galaxy_ is a group involving _Star Lord_, _Gamora_, _Drax_, _Rocket_ & _Groot_ until they split in _Avengers: Infinity War_.)


#### 2. How to simplify data management & package the project?

- Initially use plain HTML + CSS + JS, then move to node+expressJS or python solutions
- Find a better solution than plain json (django store? mongo?)

#### 3. How to achieve interactions?

Create data queries for each use case:
- Navigate by movie: include the movie attribute for each event.
- Navigate by character: maintain the character's events in a linked list, sorted by date (or filter from a join table?)