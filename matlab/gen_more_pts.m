function M2 = gen_more_pts(M)

if size(M,1) > 20
    disp('You have too many points submitted. This will take forever!!!')
    M2 = M;
    return
end
M2 = [];
for i = 1:size(M,1)-1
    x1 = M(i,1);
    x2 = M(i+1,1);
    y1 = M(i,2);
    y2 = M(i+1,2);
    x = [linspace(x1,x2,100)]';
    y = [linspace(y1,y2,100)]';
    M2 = [M2; x y];
end
    