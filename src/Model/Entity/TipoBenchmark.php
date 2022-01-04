<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * TipoBenchmark Entity
 *
 * @property int $id
 * @property string $nome
 * @property string $sigla
 *
 * @property \App\Model\Entity\IndicadoresFundo[] $indicadores_fundos
 * @property \App\Model\Entity\RelBenchmarksClasseFundo[] $rel_benchmarks_classe_fundos
 */
class TipoBenchmark extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'nome' => true,
        'sigla' => true,
        'indicadores_fundos' => true,
        'rel_benchmarks_classe_fundos' => true,
    ];
}
