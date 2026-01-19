This project is to create a transformer script witohut repeating code writting again and again. And reduce the ammount of lines in the transformer
file that I'll manually write thanks to OP functions.  However, When writting this I wnat to do something else.
Since this is a complex data manipulation task I need a way to see sample display to let me know how this is being done. So that I have a live preview while working on it how it works and how it looks and the changes.

As an example.


There should be a way the whole project is only keep the ammount of lines that we initially assing. let say we say sample_size = 10 then it should only modify those 10 during the mode is in Build mode. When it's not in the build mode it should do it to the whole thing. 
Also there should be the ability that I can mention the limit and not apply it till I call it as well. May be ya do it like that.

How this is going to affect the display.
When display as an example if I say 

    df = set_value(df, "company", "Default Company")

it should prinit that column.

Like that in every OP it should display the affected columns with whatever the index colums mentioned in the above under the INDEX_COLs constant.

If it's not mentioned then  it should just show the current index it has in the df and the colunmn that got affefcted. 

These changes are not only display changes some are OP changes you have to figure out which is which and use them properly.



Addition to that about the display I have few more things that needs to get done.
    - Marimo should be the thing that should used for the disply.
    - The code will be written by the dev on the transformer .py file nothing else and the display should automatically display them in a proper way. and to start the marimo server we will use the transformer py file marimo run commands. Make sure this is doesnt require ton of things in the transformer file. Maybe just one decorator?? Idk do this the best way possible.

    