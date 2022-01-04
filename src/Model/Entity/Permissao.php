<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * Permissao Entity
 *
 * @property int $id
 * @property bool $supor_fck_root
 * @property bool $administrador_mng
 * @property bool $carteiras_mng
 * @property bool $logs_mng
 * @property bool $operacoes_mng
 * @property bool $usuarios_mng
 * @property bool $tipos_mng
 * @property bool $rel_mng
 *
 * @property \App\Model\Entity\TipoPlano[] $tipo_planos
 */
class Permissao extends Entity
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
        'supor_fck_root' => true,
        'administrador_mng' => true,
        'carteiras_mng' => true,
        'logs_mng' => true,
        'operacoes_mng' => true,
        'usuarios_mng' => true,
        'tipos_mng' => true,
        'rel_mng' => true,
        'tipo_planos' => true,
    ];
}
