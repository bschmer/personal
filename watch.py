#! /usr/bin/env python

'''
Simple module to watch TeslaCam videos using OpenCV.

Not in the best shape at the moment but it works.
'''
import sys
import datetime
import numpy as np
import cv2
import spin

# TODO: Make an option
# TODO: Prolonged motion detecion
# TODO: Display options
# TODO: Show on motion
# TODO: Capture buffer

def framesgenerator(*paths):
    '''
    Generate the set of frames from each video.
    '''
    options = ['front', 'back', 'right_repeater', 'left_repeater']

    for path in paths:
        print path
        videos = []
        for option in options:
            videos.append(cv2.VideoCapture(path.replace('front', option)))

        while videos:
            frames = []
            for cap in videos:
                ret, frame = cap.read()
                if not ret:
                    cap.release()
                    videos.remove(cap)
                    break
                frames.append(frame)
            if len(frames) != 4:
                break
            yield frames
def main():
    '''
    Main program
    '''
    spinner = spin.IdleSpin()
    skip = 15
    sleep = 1
    counter = 0
    paths = sys.argv[1:]
    lastframe = None
    saving = False
    checkmotion = True
    scale = .55
    individual = False
    showmotion = False
    for frames in framesgenerator(*paths):
        spinner.spin()
        counter += 1
        if counter % skip == 0:
            frame = np.vstack([np.hstack(frames[:2]), np.hstack(frames[2:])])
            resize_factor = 500.0 / frame.shape[1]
            resized = cv2.resize(frame, (0, 0), fx=resize_factor, fy=resize_factor)
            gray = cv2.GaussianBlur(cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY), (21, 21), 0)
            motion = False
            if checkmotion and lastframe is not None:
                delta = cv2.absdiff(lastframe, gray)
                thresh = cv2.threshold(delta, 25, 255, cv2.THRESH_BINARY)[1]
                thresh = cv2.dilate(thresh, None, iterations=2)
                cnts = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                if len(cnts) == 2:
                    cnts = cnts[0]
                elif len(cnts) == 3:
                    cnts = cnts[1]
                else:
                    cnts = []
                for contour in cnts:
                    if showmotion:
                        (x, y, w, h) = [int(z/resize_factor) for z in cv2.boundingRect(contour)]
                        cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 2)
                    motion = True
            else:
                motion = True
            lastframe = gray
            if motion:
                spinner.shift()
                if individual:
                    for index, iframe in enumerate(frames):
                        cv2.imshow('frame%d' % index, iframe)
                else:
                    cv2.imshow('frame', cv2.resize(frame, (0, 0), fx=scale, fy=scale))
        key_pressed = cv2.waitKey(sleep)
        if key_pressed & 0xFF == ord('q'):
            cv2.destroyAllWindows()
            sys.exit(0)
        if key_pressed & 0xFF == ord('+'):
            skip = min(skip+1, 30)
            print skip
        if key_pressed & 0xFF == ord('1'):
            skip = 1
        if key_pressed & 0xFF == ord('2'):
            skip = 7
        if key_pressed & 0xFF == ord('3'):
            skip = 15
        if key_pressed & 0xFF == ord('4'):
            skip = 30
        if key_pressed & 0xFF == ord('5'):
            skip = 60
        if key_pressed & 0xFF == ord('6'):
            sleep = 1
        if key_pressed & 0xFF == ord('7'):
            sleep = 250
        if key_pressed & 0xFF == ord('8'):
            sleep = 500
        if key_pressed & 0xFF == ord('9'):
            sleep = 1000
        if key_pressed & 0xFF == ord('0'):
            sleep = 0
        if key_pressed & 0xFF == ord('f'):
            scale = 1.0
        if key_pressed & 0xFF == ord('F'):
            scale = .55
        if key_pressed & 0xFF == ord('i'):
            individual = not individual
            if individual:
                cv2.destroyWindow('frame')
            else:
                for i in range(0, 4):
                    cv2.destroyWindow('frame%d' %i)
        if key_pressed & 0xFF == ord('-'):
            skip = max(skip-1, 1)
            print skip
        if key_pressed & 0xFF == ord('p'):
            sleep = min(sleep+100, 2000)
        if key_pressed & 0xFF == ord('m'):
            sleep = max(sleep-100, 1)
        if key_pressed & 0xFF == ord('s'):
            cv2.imwrite('%s.jpg' % datetime.datetime.now().isoformat(), frame)
        if key_pressed & 0xFF == ord('w'):
            saving = not saving
        if key_pressed & 0xFF == ord('c'):
            checkmotion = not checkmotion
            print 'Check motion: %s' % checkmotion
        if saving:
            cv2.imwrite('%s.jpg' % datetime.datetime.now().isoformat(), frame)

    cv2.waitKey(0)
    cv2.destroyAllWindows()

if __name__ == '__main__':
    main()
