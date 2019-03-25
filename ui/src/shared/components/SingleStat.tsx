// Libraries
import React, {SFC} from 'react'

// Utils
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'
import {formatStatValue} from 'src/shared/utils/formatStatValue'

// Types
import {ViewType, SingleStatView} from 'src/types/v2/dashboards'

interface Props {
  properties: SingleStatView
  stat: number
}

const SingleStat: SFC<Props> = ({stat, properties}) => {
  const {prefix, suffix, colors, decimalPlaces} = properties

  const {bgColor: backgroundColor, textColor} = generateThresholdsListHexs({
    colors,
    lastValue: stat,
    cellType: ViewType.SingleStat,
  })

  const formattedValue = formatStatValue(stat, {decimalPlaces, prefix, suffix})

  return (
    <div className="single-stat" style={{backgroundColor}}>
      <div className="single-stat--resizer">
        <svg
          width="100%"
          height="100%"
          viewBox={`0 0 ${formattedValue.length * 55} 100`}
        >
          <text
            className="single-stat--text"
            fontSize="100"
            y="59%"
            x="50%"
            dominantBaseline="middle"
            textAnchor="middle"
            style={{fill: textColor}}
          >
            {formattedValue}
          </text>
        </svg>
      </div>
    </div>
  )
}

export default SingleStat
