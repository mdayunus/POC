def sumofmatrix(arr):
    num = 0
    for i in range(0, len(arr)):
        for j in range(0, len(arr)):
            if i == 1 and j != 1:
                continue
            else:
                num = num + arr[i][j]
    return num



def creatematrix(inpArr,l,b):
    newArr = []
    newinnweArr = []
    
    for i in range(l,l+3):
        for j in range(b,b+3):
            newinnweArr.append(inpArr[i][j])
        newArr.append(newinnweArr)
        newinnweArr = []
    return newArr

def maxofmatrix(inpArr):
    maxi = -1000
    matrix = []
    for i in range(0,4):
        for j in range(0,4):
            matrix.append(creatematrix(inpArr,i,j))
    print(matrix)
    for m in matrix:
        if sumofmatrix(m) > maxi:
            maxi = sumofmatrix(m)

    return maxi

                
if __name__ == '__main__':
    a = [
        [1, 1, 1, 0, 0, 0],
        [0, 1, 0, 0, 0, 0],
        [1, 1, 1, 0, 0, 0],
        [0, 0, 2, 4, 4, 0],
        [0, 0, 0, 2, 0, 0],
        [0, 0, 1, 2, 4, 0],
    ]
    print(maxofmatrix(a))

    b = [
        [-1, -1, 0, -9, -2, -2],
        [-2, -1, -6, -8, -2, -5],
        [-1, -1, -1, -2, -3, -4],
        [-1, -9, -2, -4, -4, -5],
        [-7, -3, -3, -2, -9, -9],
        [-1, -3, -1, -2, -4, -5]
    ]

    # print(maxofmatrix(b))



    c = [[1, 1, 0], [1, 1, 1], [1, 1, 1]]



    

    




# hg = 1,1 1,2 1,3 2,2 3,1 3,2 3,3
