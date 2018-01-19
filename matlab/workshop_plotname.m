%% UROP Matlab Workshop
% @author: Alex Cao, University of Michigan
% Email: caoa AT umich DOT edu
% Consulting for Statistics, Computing, and Analytics Research (CSCAR)
% MATLAB Version: 9.0.0.370719 (R2016a)
% Operating System: Microsoft Windows 7 Enterprise  Version 6.1 (Build 7601: Service Pack 1)
% Java Version: Java 1.7.0_60-b19 with Oracle Corporation Java HotSpot(TM) 64-Bit Server VM mixed mode

% Students can install a free version of Matlab on their PC
% https://www.itcs.umich.edu/sw-info/math/MATLABStudents.html

%% Start with a clean slate
clear; close all

%% Creating variables
a = 3.14
b = 'this is a string'
c = [2 4;
     6 8]

%% Built-In functions and constants
% Constant
d = pi
% Imaginary numbers
e = sqrt(-9)
% Creating imaginary numbers
f = 1-2i

%% Creating vectors and matrices
% creating a row vector
row_vector = [2 4 6 8 10]
% creating a column vector
col_vector = [1; 
              3;
              5;
              7;
              9]
% transpose
row_vector = row_vector' % or row_vector = [2 4 6 8 10]'
% creating a matrix
matrix = [9 8 7;
          6 5 4;
          3 2 1]
% Adding rows or columns to an existing matrix or vector
v = [10 20 30]
addrow = [matrix;
          v]
addcol = [matrix v'] 
% Deleting rows or columns from an existing matrix or vector
addrow(end,:) = []
addcol(:,4) = []

%% Selecting and accessing data
% Select column(s) of data
a = matrix(:,1)
% To select multiple columns
b = matrix(:,2:3)
% Columns do not even have to be continuous
c = matrix(:,[3 1])
% Exact same thing for rows
d = matrix(1,:)
e = matrix(2:3,:)
f = matrix([3 1],:)

%% Plotting
M = magic(3) % magic square
plot(M(:,1),M(:,2),'o-')

%% Exercise 1 (5 minutes)
% Task 1: Construct a matrix of points to spell out the first letter of your name
% Task 2: Plot the letter
% For example,
% Task 1
A = [0 0;
    1 4;
    2 0;
    1.5 2;
    0.5 2];
% Task 2
plot(A(:,1),A(:,2),'x-')

%% Running external Matlab programs
% Just type the name of the m-file (should not have any spaces)
% Run letters m-file to get custom block font alphabet by author
letters

%% We will plot our name in Matlab
% Grab your letters from the alphabet (cell array) using the index number
A = alphabet{1};
L = alphabet{12};
E = alphabet{5};
X = alphabet{24};

%% Matrix
% Letters are stored as a Nx2 matrix
% First column are the x-coordinates
% Second column are the y-coordinates
A

%% Plotting your name

% Create a new cell array variable with our letters
name = {A,L,E,X};
% Close previous figure
close
% Open new figure
figure(1)
% Iterate through the letters using a for loop
for i = 1:length(name)
    % Grab a letter
    letter = name{i};
    % Get x and y column
    x = letter(:,1);
    y = letter(:,2);
    % Plot letter with a blue line
    plot(x,y,'b-');
    % Set axis limits
    ylim([-1 5])
    axis equal
    % Do not overwrite previous plots
    hold on
end

% Create labels
xlabel('x-axis')
ylabel('y-axis')
title('Plotting My Name')

%% 
% In order to see all the letters clearly, we need to offset the letters
% We'll use matrix addition/subtraction to create the offset
% Creating a constant offset is easy
close; figure(2)
for i = 1:length(name)
    letter = name{i};
    % add offset to the x-coordinate based on letter position
    x = letter(:,1) + i*2.5;
    y = letter(:,2);
    % plot letter with red dash dot line and circle markers
    plot(x,y,'r-.o');
    hold on
end
% Alternate way to set axis limits
axis([-1 15 -1 5])

% See the following URLs for different point and line options
% http://www.mathworks.com/help/matlab/ref/plot.html#inputarg_LineSpec

%% 
% You can also add a vector or matrix (instead of a constant) to a matrix
% (i.e. letter)
F = alphabet{6}
plot(F(:,1),F(:,2),'g','linewidth',2)
%%
% Here we add a vector to change the first row (i.e. bottom point)
F(1,:) = F(1,:)+[1 1]
% plot a green line with a linewidth of 2
plot(F(:,1),F(:,2),'g','linewidth',2)

%% 
% We can also scale the letters so that they are smaller or bigger
% We'll use matrix element multiplication to accomplish the scaling
close; figure(3)
for i = 1:length(name)
    letter = name{i};
    % same x-offset as before
    x = letter(:,1) + i*2.5;
    % scale the y-coordinate by multiplication of an exponential
    y = letter(:,2) * exp(+i/5);
    % plot black line with diamond markers
    plot(x,y,'k-d');
    hold on
end
% Alternate way to set axis limits
xlim([0 15])
ylim([-1 10])

%% Exercise 2 (5 minutes)
% Task 1: Copy the code section above 
% Task 2: Plot your name vertically by using matrix addition/subtraction
% Task 3: Shrink the letters in your name by using matrix-element
% multiplication/division and re-plot it

%% Animation 
% Here's how to animate the letters sequentially
close; figure(4)
axis([-1,15,-1,10])

% Set time delay between drawing lines
time_delay = 0.5;

% Create empty cell array for animated objects
object = {};
% Iterate thru the letters
for i = 1:length(name)
    % Create animated lined object for each letter and save it to cell array
    object{i} = animatedline;
    letter = name{i};
    x = letter(:,1) + i*2.5;
    y = letter(:,2) * exp(+i/5);
    % Iterate through each point defining our letter and draw it
    for j = 1:length(letter)
        addpoints(object{i},x(j),y(j));
        drawnow
        pause(time_delay)
    end
end

%% 
% To produce smoother animation, we need more points to plot
% Make lines with more points (say 100)
num_of_pts = 100;
% Create an evenly spaced vector using linspace
% linspace(start,end,number of points)
x1 = linspace(0,1,num_of_pts);
y1 = linspace(0,4,num_of_pts);
x2 = linspace(1,2,num_of_pts);
y2 = linspace(4,0,num_of_pts);
x3 = linspace(1.5,0.5,num_of_pts);
y3 = linspace(2,2,num_of_pts);

% quote symbol does a transpose of the matrix
% we want to convert from a row vector to a column vector
% we concatenate the vectors side by side and then on top of each
% other
A = [x1' y1';
     x2' y2';
     x3' y3'];

% get size of A
size(A)

%% Exercise 3 (5 minutes)
% Task 1: Similar to the code section above, construct a matrix for the
% letter T using linspace with 100 pts. Hint: You need to create x1, y1,
% x2, y2 for the vertical and horizontal lines
% Task 2: Plot the matrix using x markers (e.g. plot(x,y,'x') ) and set the
% axis so that the letter is not touching a border

%% Redraw letter A with more points and no time delay
close; figure(5)
hA = animatedline;
axis([-1,12,-1,5])

for k = 1:length(A)
    addpoints(hA,A(k,1),A(k,2));
    drawnow
end

%% Smoother Animation 
% I've written a matlab function gen_more_pts.m to add more points to
% letters for you. Let's use it to animate our names.
close; figure(6)
axis([-1,15,-1,10])

% Create empty cell array for animated objects
object = {};
% For loop for adding points to a line
for i = 1:length(name)
    % Create animated lined object for each letter and save it to cell array
    object{i} = animatedline;
    letter = name{i};
    % gen_more_pts function creates more points for uss
    animate_letter = gen_more_pts(letter);
    x = animate_letter(:,1) + i*2.5;
    y = animate_letter(:,2) * exp(+i/5);
    for j = 1:length(animate_letter)
        addpoints(object{i},x(j),y(j));
        drawnow
    end
end

%% Exercise 4 (5 minutes)
% Task 1: Copy the code section above
% Task 2: Animate the vertical version of your name

%% Plot Attributes
% You can change the look of your lines after they are plotted by accessing
% their attributes such as Color or LineWidth or LineStyle
% To get a list of a plot attributes, use the get command
get(object{1})
% You can also type "object{1}." followed by a tab to get a dropdown list
% To make something invisible use the Visible attribute
object{1}.Visible = 'off'
% To make something visible again
object{1}.Visible = 'on'
% If you don't know what options are available to you for a specific
% attribute, you can use the set command
set(object{1})

%% Generating Random Numbers and using a random seed
% We will use random numbers to randomly change the attributes of our plot
% Use a seed so that you get a predictable sequence of numbers
% rng(56789)
linestyle_options = {'-','--',':','-.'};
for i = 1:12
    % generate one random integer for which letter to modify
    n = randi(length(name),1);
    % generate a 3x1 vector of random numbers from (0,1)
    color = rand(3,1)
    object{n}.Color = color;
    % generate one random integer for the linewidth
    object{n}.LineWidth = randi(10,1);
    % generate one random integer for the linestyle
    index = randi(length(linestyle_options),1);
    object{n}.LineStyle = linestyle_options{index};
    pause(1)
end

%% Exercise 5 (5 minutes)
% Task 1: Generate a 4x1 vector of random integers from 1 to 10
% Task 2: Take the sum of it
% Task 3: Repeat 1 & 2
% Task 4: Set a seed for the random generator using your favourite number
% Task 5: Redo 1,2,3

%% Let's redraw our name 
close; figure(7)
for i = 1:length(name)
    letter = name{i};
    animate_letter = gen_more_pts(letter);
    x = animate_letter(:,1) + i*2.5;
    y = animate_letter(:,2);
    % plot letters as magenta line with pentagon markers
    plot(x,y,'m-p');
    hold on
end
% Alternate way to set axis limits
axis([0 15 -1 5])

%% Now suppose we wanted to cut our name (i.e. points) into half
% We can segment our name using logical indexing
close; figure(8)
y_cutoff = 2.5;
for i = 1:length(name)
    letter = name{i};
    animate_letter = gen_more_pts(letter);
    % original matrix size
    disp(size(animate_letter))
    % generate boolean of pts meeting criterion
    index = animate_letter(:,2) < y_cutoff;
    % grab matching pts using indices
    animate_letter = animate_letter(index,:);
    % new matrix size (should be smaller)
    disp(size(animate_letter))
    x = animate_letter(:,1) + i*2.5;
    y = animate_letter(:,2);
    plot(x,y,'m-p');
    hold on
end
axis([0 15 -1 5])

%% You can use more than one logical operation at a time
close; figure(9)
% & means AND
% | means OR
y_cutoff = 2.5;
for i = 1:length(name)
    letter = name{i};
    animate_letter = gen_more_pts(letter);
    size(animate_letter)
    % AND statement joining two criteria
    index = (animate_letter(:,2) < y_cutoff) & (animate_letter(:,2) > 1.25);
    animate_letter = animate_letter(index,:);
    size(animate_letter)
    x = animate_letter(:,1) + i*2.5;
    y = animate_letter(:,2);
    plot(x,y,'m-p');
    hold on
end
axis([0 15 -1 5])

%% Exercise 6 (5 minutes)
% Task 1: Copy the code section above
% Task 2: Only show the portion of your name that is less than 1 or greater
% than 2 on the y-axis

%% Import Data Demo
% There are many functions to import data into Matlab from external sources
% Some choices are: uiimport, load, importdata, textscan, dlmread, fread,
% fscanf, readtable, xlsread
%
% The most friendly method to beginners is uiimport which acts like excel
uiimport('crash.txt')

%% Exercise 7 (10 minutes)
% Task 1: Import the CrashSeverity column into the workspace
% Task 2: Extract the fatal crashes (value = 1) using logical indexing
% Task 3: Count how many fatal crashes there are in dataset
% Task 4: Import the Longitude/Latitude columns into the workspace
% Task 5: Plot Longitude/Latitude coordinates using any triangle marker.
% Are there any bad data points? 
% Tip: Longitude should be negative in this case.
% Task 6: Remove the bad points using logical indexing and re-plot the
% coordinates using a triangle marker


%% Some useful Matlab commands to know
% Saving your work
% Saving variables in your workspace
save workshop.mat
% Clear the workspace
clear
% Reload everything
load workshop.mat
% If you just want to save a couple of variables
save workshop X E L A
% close last figure
close
% close all figures
close all
% clear command window
clc
% bring up command history
commandhistory
% Last unassigned answer in command window
ans

%% Formatting output
z = 1534513546
% To change the look of the output, use the format function
format longg
z
% To change back to the default format
format

%% Getting Help
% help for a function
help plot
doc plot
% Bring up Matlab examples
demo
% You can also use the search bar in the top right corner or use the *?* 
% icon next to it to open up an equivalent window

%% References
% MathWork (makers of Matlab) Resources
 
% Matlab tutorials from MathWorks 
% https://www.mathworks.com/support/learn-with-matlab-tutorials.html
% http://www.mathworks.com/help/matlab/getting-started-with-matlab.html

% Matlab Forum for Q&A
% http://www.mathworks.com/matlabcentral/answers/ 

% Cody: Challenge yourself to Matlab coding problems
% http://www.mathworks.com/matlabcentral/cody

% PDF tutorial
% https://www.mathworks.com/help/pdf_doc/matlab/getstart.pdf

% 3rd Party Add-Ons
% http://www.mathworks.com/matlabcentral/fileexchange/

% Matlab Blogs
% http://blogs.mathworks.com Matlab Blog

% Matlab Toolboxes
% https://www.mathworks.com/products/

% To see what is installed on your version of Matlab, use the ver
% command
ver

%% Other Matlab Resources

% Interactive course by the University of Edinburgh
% http://www.see.ed.ac.uk/teaching/courses/matlab/ 

% Free online book
% http://greenteapress.com/matlab/


%% Other Fun Stuff

%% Alternate way to do animation
% Rotate our name
% Let's plot our name again
% The plot command will be outside the for loop this time
close; figure(100)
alex = [];
for i = 1:length(name)
    letter = name{i};
    x = letter(:,1) + i*2.5;
    y = letter(:,2);
    alex = [alex; x y];
end
hAlex = plot(alex(:,1),alex(:,2),'linewidth',2,'color',[0.7 0.2 0.5]);
axis([-12 12 -12 12])

%%
% Set the DataSource attribute to this variable
hAlex.XDataSource = 'rotateAlex(:,1)';
hAlex.YDataSource = 'rotateAlex(:,2)';
% Create an evenly spaced vector from 0 to 2*pi for rotation
th = linspace(0,2*pi,500);

%% Rotate about z-axis
for i = 1:length(th)
    % Angle
    theta = th(i);
    % Rotation matrix about z-axis
    Rz = [cos(theta) -sin(theta);
        sin(theta)  cos(theta)];
    % Matrix multiplication of rotation matrix with name points
    rotateAlex = (Rz*alex')';
    % Update figure handle
    refreshdata(hAlex)
    % Pause in seconds
    pause(0.01)
end

%% Center my name around the origin
% use repmat to duplicate 2x1 vector
alex2 = alex - repmat(mean(alex),size(alex,1),1);
% Add the z-value of zero to my name points
alex2 = [alex2 zeros(size(alex2,1),1)];

%% Rotate about y-axis
for i = 1:length(th)
    theta = th(i);
    Ry = [cos(theta) 0 sin(theta);
          0 1 0;
         -sin(theta) 0 cos(theta)];
    rotateAlex = (Ry*alex2')';
    refreshdata(hAlex)
    pause(0.01)
end

%% Rotate about x-axis
% Move my name around some more
alex2(:,2) = alex2(:,2) + min(alex2(:,2));
for i = 1:length(th)
    theta = th(i);
    Rx = [1 0 0;
          0 cos(theta) -sin(theta);
          0 sin(theta)  cos(theta)];
    rotateAlex = (Rx*alex2')';
    refreshdata(hAlex)
    pause(0.01)
end

%% Animation of a helix
n = 5000; % determines how many pts to draw
xc = 3; yc = 3;
r = linspace(1,6,n); % radius
t = linspace(0,12*pi,n); % how many loops to make
x = 0.8*r.*cos(t) + xc;
y = r.*sin(t) + yc;
z = linspace(0,5,n);
v = linspace(0.001,1,n);
close all; figure(101)
h = animatedline;
axis([-10,10,-10,10,0 5])
grid on
xlabel('X'); ylabel('Y'); zlabel('Z')
for k = 1:n
    h.LineWidth = (v(k)+1)*4;
    h.Color = [v(k) 1-v(k) v(k)];
    addpoints(h,x(k),y(k),z(k));
    % Set viewing angle
    view(-mod(k/120,90),90-mod(k/72,70))
    drawnow
end
