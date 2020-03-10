#! /usr/bin/env python

import os
import sys
import glob
import numpy as np
import spin
import cv2

options = ['front', 'back', 'left_repeater', 'right_repeater']

# TODO: Make an option
# TODO: Prolonged motion detecion
# TODO: Display options
# TODO: Show on motion
# TODO: Capture buffer

def framesgenerator(*paths):
    for path in paths:
        print path
        videos = []
        for option in options:
            videos.append(cv2.VideoCapture(path.replace('front', option)))

        while videos:
            frames = []
            for index, cap in enumerate(videos):
                ret, frame = cap.read()
                if not ret:
                    cap.release()
                    videos.remove(cap)
                    break
                frames.append(frame)
            if len(frames) != 4:
                break
            yield frames
spinner = spin.IdleSpin()
skip = 15
counter = 0
paths = sys.argv[1:]
lastframe = None
for frames in framesgenerator(*paths):
    spinner.spin()
    counter += 1
    if counter % skip == 0:
        frame = np.vstack([np.hstack(frames[:2]), np.hstack(frames[2:])])
        resize_factor = 500.0 / frame.shape[1]
        resized = cv2.resize(frame, (0, 0), fx=resize_factor, fy=resize_factor)
        gray = cv2.GaussianBlur(cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY), (21, 21), 0)
        motion = False
        if lastframe is not None:
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
            for c in cnts:
                if False:
                    print cv2.contourArea(c)
                if False:
                    (x, y, w, h) = [int(z/resize_factor) for z in cv2.boundingRect(c)]
                    cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 2)
                motion = True
        else:
            motion = True
        if False:
            print '<' * 25, gray.shape
            if True:
                cv2.imshow('gray', gray)
                rv = cv2.waitKey(1)
        lastframe = gray
        if motion:
            cv2.imshow('frame', frame)
            rv = cv2.waitKey(1)
            if rv & 0xFF == ord('q'):
                break
            if rv & 0xFF == ord('+'):
                skip = min(skip+1, 30)
                print skip
            if rv & 0xFF == ord('-'):
                skip = max(skip-1, 1)
                print skip

cv2.waitKey(0)
cv2.destroyAllWindows()
