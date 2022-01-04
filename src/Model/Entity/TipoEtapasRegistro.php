<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * TipoEtapasRegistro Entity
 *
 * @property int $id
 * @property int $fase
 * @property string $etapa
 * @property bool $podeLogar
 * @property bool $podeInvestir
 *
 * @property \App\Model\Entity\Usuario[] $usuarios
 */
class TipoEtapasRegistro extends Entity
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
        'fase' => true,
        'etapa' => true,
        'podeLogar' => true,
        'podeInvestir' => true,
        'usuarios' => true,
    ];
}
