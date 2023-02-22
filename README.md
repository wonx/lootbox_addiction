# GameGuard

![](https://github.com/wonx/lootbox_addiction/blob/main/flask_app/static/gameguard_composite.png?raw=true)

<sub><a href="https://www.freepik.com/free-vector/computer-design_919225.htm#query=computer%20screen&position=1&from_view=search&track=ais">Image by d3images</a> on Freepik</sub>

## What is it?
GameGuard is a machine learning solution designed to prevent video game addiction linked to [microtransactions](https://en.wikipedia.org/wiki/Microtransaction). It tracks and mitigates addictive behavior in real-time by monitoring and analyzing user purchase patterns for [loot boxes](https://en.wikipedia.org/wiki/Loot_box), a kind of microtransaction in which the item the player obtain is randomized.

The use of microtransactions, especially in loot boxes, can lead to addictive behavior and excessive spending among players. GameGuard was created to address the problematic aspects of microtransactions that negatively affect the game developer industry and to provide a solution that considers the mental health issues associated with video games.

## How does it work?
GameGuard uses open data about [loot box purchases](https://www.csgo.com.cn/api/lotteryHistory) for the game Counter Strike in the Chinese market, which includes information on the timestamp, username, and outcome of each purchase. The value for each lootbox and its outcome has been scrapped from an [online market](https://wiki.cs.money/) where people buy and sell these lootboxes.

It also uses a [labeled dataset on online gambling](http://www.thetransparencyproject.org/download_index.php) to train a machine learning classifier that identifies users who are at risk of addiction in the lootbox dataset.

The solution is accessible through an online dashboard, which provides an overview of the lootbox purchase patterns and shows the users with the higher risk score. On each user's profile page, the evolution of the risk score through time is shown, together with daily statistics of their purchase history, with the possibility of inspecting the raw data. All those features are designed to allow regulatory agencies to take necessary actions to prevent addiction.

## I want to try it!
For an online demo, just head to http://gameguard.marcpalaus.com

Otherwise, follow the [Installation](#installation) instructions below to run GameGuard in your own machine.

## How can I contribute?
- ⭐ **Star** this repository if you like this project!
- 🧪 **Testing**: Try it! Follow the installation instructions and try it for yourself. 
- 🐞 **Issue reporting**:  Found a bug? Help us improve by reporting it!
- 💡 **Request features**: Do you miss something important? Submit a feature request!

## Installation

[to do]
